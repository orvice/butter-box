package cursor

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/structpb"

	cursorv1 "github.com/orvice/butter-box/pkg/proto/butterbox/cursor/v1"
	cursorv1connect "github.com/orvice/butter-box/pkg/proto/butterbox/cursor/v1/cursorv1connect"
	sdkv1 "github.com/orvice/butter-box/pkg/proto/sdk/v1"
	sdkv1connect "github.com/orvice/butter-box/pkg/proto/sdk/v1/v1connect"
)

// The test binary doubles as a cursor-sdk-bridge executable. This exercises
// the real process handshake and generated Connect clients without requiring a
// Cursor API key or network access to Cursor.
func TestMain(m *testing.M) {
	if os.Getenv("FAKE_CURSOR_BRIDGE") == "1" {
		fakeCursorBridgeMain()
		return
	}
	os.Exit(m.Run())
}

type fakeBridgeControl struct {
	sdkv1connect.UnimplementedSdkBridgeControlServiceHandler
	server *http.Server
}

func (f *fakeBridgeControl) Ping(context.Context, *connect.Request[sdkv1.PingRequest]) (*connect.Response[sdkv1.PingResponse], error) {
	return connect.NewResponse(&sdkv1.PingResponse{Message: "pong"}), nil
}

func (f *fakeBridgeControl) GetVersion(context.Context, *connect.Request[sdkv1.GetVersionRequest]) (*connect.Response[sdkv1.GetVersionResponse], error) {
	return connect.NewResponse(&sdkv1.GetVersionResponse{
		BridgeVersion:   "1.0.30-test",
		ProtocolVersion: "sdk.v1",
	}), nil
}

func (f *fakeBridgeControl) Shutdown(context.Context, *connect.Request[sdkv1.ShutdownRequest]) (*connect.Response[sdkv1.ShutdownResponse], error) {
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = f.server.Shutdown(context.Background())
	}()
	return connect.NewResponse(&sdkv1.ShutdownResponse{}), nil
}

type fakeBridgeAgent struct {
	sdkv1connect.UnimplementedSdkAgentServiceHandler

	mu      sync.Mutex
	nextRun int
	cancels map[string]chan struct{}
	agentID string
}

func (f *fakeBridgeAgent) CreateAgent(_ context.Context, req *connect.Request[sdkv1.CreateAgentRequest]) (*connect.Response[sdkv1.CreateAgentResponse], error) {
	if req.Msg.GetOptions().GetApiKey() != "test-api-key" {
		return nil, fakeAPIKeyError()
	}
	f.agentID = "fake-agent-1"
	writeJSONLine(os.Getenv("FAKE_CURSOR_CREATE_LOG"), map[string]any{
		"agent_id": f.agentID,
		"name":     req.Msg.GetOptions().GetName(),
		"model":    req.Msg.GetOptions().GetModel().GetId(),
		"mode":     int32(req.Msg.GetOptions().GetMode()),
		"cwd":      req.Msg.GetOptions().GetLocal().GetCwd(),
	})
	return connect.NewResponse(&sdkv1.CreateAgentResponse{
		AgentId: f.agentID,
		Model:   req.Msg.GetOptions().GetModel(),
	}), nil
}

func (f *fakeBridgeAgent) ResumeAgent(_ context.Context, req *connect.Request[sdkv1.ResumeAgentRequest]) (*connect.Response[sdkv1.ResumeAgentResponse], error) {
	if req.Msg.GetOptions().GetApiKey() != "test-api-key" {
		return nil, fakeAPIKeyError()
	}
	if req.Msg.GetAgentId() != "fake-agent-1" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("agent not found"))
	}
	f.agentID = req.Msg.GetAgentId()
	return connect.NewResponse(&sdkv1.ResumeAgentResponse{AgentId: f.agentID}), nil
}

func (f *fakeBridgeAgent) GetAgent(_ context.Context, req *connect.Request[sdkv1.GetAgentRequest]) (*connect.Response[sdkv1.GetAgentResponse], error) {
	if req.Msg.GetOptions().GetApiKey() != "test-api-key" {
		return nil, fakeAPIKeyError()
	}
	return connect.NewResponse(&sdkv1.GetAgentResponse{
		Agent: &sdkv1.SdkAgentInfo{
			AgentId:     req.Msg.GetAgentId(),
			RuntimeInfo: &sdkv1.SdkAgentInfo_Local{Local: &sdkv1.LocalAgentInfo{Cwd: currentWorkingDirectory()}},
		},
	}), nil
}

func (f *fakeBridgeAgent) Send(ctx context.Context, req *connect.Request[sdkv1.SendRequest], stream *connect.ServerStream[sdkv1.RunStreamMessage]) error {
	f.mu.Lock()
	f.nextRun++
	runID := fmt.Sprintf("fake-run-%d", f.nextRun)
	cancel := make(chan struct{})
	f.cancels[runID] = cancel
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.cancels, runID)
		f.mu.Unlock()
	}()

	if err := stream.Send(runMessage(&sdkv1.SdkMessage{
		Type:    "system",
		Message: mustStruct(map[string]any{"run_id": runID, "agent_id": req.Msg.GetAgentId()}),
	})); err != nil {
		return err
	}
	if req.Msg.GetMessage().GetText() == "crash" {
		os.Exit(42)
	}
	if req.Msg.GetMessage().GetText() == "slow" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cancel:
			return stream.Send(runResultMessage(req.Msg.GetAgentId(), runID, sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_CANCELLED, ""))
		}
	}
	if req.Msg.GetMessage().GetText() == "failed" {
		_ = stream.Send(runMessage(&sdkv1.SdkMessage{
			Type:    "status",
			Message: mustStruct(map[string]any{"message": "the fake run failed"}),
		}))
		if err := stream.Send(runResultMessage(req.Msg.GetAgentId(), runID, sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR, "")); err != nil {
			return err
		}
		return stream.Send(runDoneMessage(req.Msg.GetAgentId(), runID))
	}

	if path := os.Getenv("FAKE_CURSOR_IMAGE_LOG"); path != "" {
		images := make([]map[string]string, 0, len(req.Msg.GetMessage().GetImages()))
		for _, image := range req.Msg.GetMessage().GetImages() {
			data, err := base64.StdEncoding.DecodeString(image.GetData().GetData())
			if err != nil {
				return err
			}
			images = append(images, map[string]string{
				"mime_type": image.GetData().GetMimeType(),
				"data":      string(data),
			})
		}
		writeJSONLine(path, map[string]any{"images": images})
	}

	text := "echo: " + req.Msg.GetMessage().GetText()
	if err := stream.Send(runMessage(&sdkv1.SdkMessage{
		Type: "assistant",
		Message: mustStruct(map[string]any{
			"message": map[string]any{
				"content": []any{map[string]any{"type": "text", "text": text}},
			},
		}),
	})); err != nil {
		return err
	}
	if err := stream.Send(runResultMessage(req.Msg.GetAgentId(), runID, sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED, text)); err != nil {
		return err
	}
	return stream.Send(runDoneMessage(req.Msg.GetAgentId(), runID))
}

func (f *fakeBridgeAgent) CancelRun(_ context.Context, req *connect.Request[sdkv1.CancelRunRequest]) (*connect.Response[sdkv1.CancelRunResponse], error) {
	f.mu.Lock()
	cancel, ok := f.cancels[req.Msg.GetRunId()]
	f.mu.Unlock()
	if ok {
		close(cancel)
	}
	return connect.NewResponse(&sdkv1.CancelRunResponse{}), nil
}

type fakeBridgeCursor struct {
	sdkv1connect.UnimplementedSdkCursorServiceHandler
}

func (fakeBridgeCursor) ListModels(_ context.Context, req *connect.Request[sdkv1.ListModelsRequest]) (*connect.Response[sdkv1.ListModelsResponse], error) {
	if req.Msg.GetOptions().GetApiKey() != "test-api-key" {
		return nil, fakeAPIKeyError()
	}
	return connect.NewResponse(&sdkv1.ListModelsResponse{Items: []*sdkv1.SdkModel{
		{Id: "composer-test", DisplayName: "Composer Test"},
	}}), nil
}

func fakeCursorBridgeMain() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(2)
	}
	tokenFile, err := os.CreateTemp("", "fake-cursor-bridge-token-")
	if err != nil {
		os.Exit(2)
	}
	defer os.Remove(tokenFile.Name())
	const token = "fake-bridge-token"
	if _, err := tokenFile.WriteString(token + "\n"); err != nil {
		os.Exit(2)
	}
	_ = tokenFile.Close()

	server := &http.Server{}
	control := &fakeBridgeControl{server: server}
	agent := &fakeBridgeAgent{cancels: map[string]chan struct{}{}}
	cursor := fakeBridgeCursor{}
	mux := http.NewServeMux()
	for path, handler := range fakeHandlers(control, agent, cursor) {
		mux.Handle(path, fakeAuth(handler, token))
	}
	server.Handler = mux

	writeJSONLine(os.Getenv("FAKE_CURSOR_SPAWN_LOG"), map[string]any{
		"cwd":  currentWorkingDirectory(),
		"args": os.Args[1:],
	})
	discovery := map[string]any{
		"schemaVersion": 1,
		"serverVersion": "1.0.30-test",
		"transport":     "tcp",
		"protocol":      "connect",
		"host":          "127.0.0.1",
		"port":          listener.Addr().(*net.TCPAddr).Port,
		"url":           "http://" + listener.Addr().String(),
		"authTokenFile": tokenFile.Name(),
	}
	data, _ := json.Marshal(discovery)
	_, _ = fmt.Fprintf(os.Stderr, "%s%s\n", bridgeReadyPrefix, data)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		os.Exit(3)
	}
}

func fakeHandlers(control *fakeBridgeControl, agent *fakeBridgeAgent, cursor fakeBridgeCursor) map[string]http.Handler {
	controlPath, controlHandler := sdkv1connect.NewSdkBridgeControlServiceHandler(control)
	agentPath, agentHandler := sdkv1connect.NewSdkAgentServiceHandler(agent)
	cursorPath, cursorHandler := sdkv1connect.NewSdkCursorServiceHandler(cursor)
	return map[string]http.Handler{
		controlPath: controlHandler,
		agentPath:   agentHandler,
		cursorPath:  cursorHandler,
	}
}

func fakeAuth(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"Unauthorized"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func fakeAPIKeyError() error {
	err := connect.NewError(connect.CodeUnauthenticated, errors.New("invalid user API key"))
	detail, _ := connect.NewErrorDetail(&sdkv1.SdkErrorDetails{
		SdkErrorCode: sdkv1.SdkErrorCode_SDK_ERROR_CODE_UNAUTHORIZED,
		Message:      "invalid user API key",
	})
	err.AddDetail(detail)
	return err
}

func runMessage(message *sdkv1.SdkMessage) *sdkv1.RunStreamMessage {
	return &sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_SdkMessage{SdkMessage: message}}
}

func runResultMessage(agentID, runID string, status sdkv1.RunLifecycleStatus, text string) *sdkv1.RunStreamMessage {
	return &sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_Result{Result: &sdkv1.RunStreamResult{
		AgentId: agentID,
		RunId:   runID,
		Status:  status,
		Result:  &sdkv1.RunResult{AgentId: agentID, RunId: runID, Status: status, Result: text},
	}}}
}

func runDoneMessage(agentID, runID string) *sdkv1.RunStreamMessage {
	return &sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_Done{Done: &sdkv1.RunStreamDone{AgentId: agentID, RunId: runID}}}
}

func mustStruct(values map[string]any) *structpb.Struct {
	result, err := structpb.NewStruct(values)
	if err != nil {
		panic(err)
	}
	return result
}

func currentWorkingDirectory() string {
	cwd, _ := os.Getwd()
	return cwd
}

func writeJSONLine(path string, value any) {
	if path == "" {
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(data, '\n'))
}

func newFakeCursorManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	t.Setenv("FAKE_CURSOR_BRIDGE", "1")
	if cfg.Bin == "" {
		cfg.Bin = os.Args[0]
	}
	if cfg.APIKey == "" {
		cfg.APIKey = "test-api-key"
	}
	if cfg.SandboxRoot == "" {
		cfg.SandboxRoot = t.TempDir()
	}
	manager := NewManager(testLogger(t), cfg)
	t.Cleanup(manager.Stop)
	return manager
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

func newCursorServiceClient(t *testing.T, manager *Manager) cursorv1connect.CursorServiceClient {
	t.Helper()
	path, handler := cursorv1connect.NewCursorServiceHandler(NewService(manager))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return cursorv1connect.NewCursorServiceClient(server.Client(), server.URL)
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func readJSONLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	return result
}

func fakeServiceErrorInfo(err error) (*errdetails.ErrorInfo, bool) {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return nil, false
	}
	for _, detail := range connectErr.Details() {
		if detail.Type() != "google.rpc.ErrorInfo" {
			continue
		}
		value, valueErr := detail.Value()
		if valueErr != nil {
			continue
		}
		info, ok := value.(*errdetails.ErrorInfo)
		return info, ok
	}
	return nil, false
}

func TestCursorServiceWireRoundTrip(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "repo")
	if err := os.Mkdir(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	createLog := filepath.Join(t.TempDir(), "create.jsonl")
	imageLog := filepath.Join(t.TempDir(), "image.jsonl")
	t.Setenv("FAKE_CURSOR_CREATE_LOG", createLog)
	t.Setenv("FAKE_CURSOR_IMAGE_LOG", imageLog)

	manager := newFakeCursorManager(t, Config{SandboxRoot: root, MaxSessions: 2})
	client := newCursorServiceClient(t, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	created, err := client.CreateSession(ctx, connect.NewRequest(&cursorv1.CreateSessionRequest{
		Name:  "wire-test",
		Model: "composer-test",
		Mode:  "plan",
		Cwd:   "repo",
	}))
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if created.Msg.GetSessionId() != "fake-agent-1" {
		t.Fatalf("session id = %q", created.Msg.GetSessionId())
	}

	response, err := client.SendMessage(ctx, connect.NewRequest(&cursorv1.SendMessageRequest{
		SessionId: created.Msg.GetSessionId(),
		Message:   "hello over wire",
		Images:    []*cursorv1.ImageContent{{MimeType: "image/test", Data: []byte("image-bytes")}},
	}))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if response.Msg.GetText() != "echo: hello over wire" {
		t.Fatalf("text = %q", response.Msg.GetText())
	}

	createRecords := readJSONLines(t, createLog)
	if len(createRecords) != 1 || createRecords[0]["model"] != "composer-test" {
		t.Fatalf("create records = %v", createRecords)
	}
	if createRecords[0]["mode"] != float64(sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN) {
		t.Fatalf("create mode = %v", createRecords[0]["mode"])
	}
	cwdRecords, ok := createRecords[0]["cwd"].([]any)
	if !ok || len(cwdRecords) != 1 || cwdRecords[0] != cwd {
		t.Fatalf("create cwd = %v, want %q", createRecords[0]["cwd"], cwd)
	}
	imageRecords := readJSONLines(t, imageLog)
	images := imageRecords[0]["images"].([]any)
	image := images[0].(map[string]any)
	if image["mime_type"] != "image/test" || image["data"] != "image-bytes" {
		t.Fatalf("image = %v", image)
	}
}

func TestCursorErrorsAndCapacity(t *testing.T) {
	manager := newFakeCursorManager(t, Config{MaxSessions: 1})
	id, err := manager.Create(context.Background(), CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("empty session id")
	}
	if _, err := manager.Create(context.Background(), CreateOpts{}); !errors.Is(err, ErrTooManySessions) {
		t.Fatalf("second Create error = %v, want capacity error", err)
	}
	if err := manager.Abort(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Abort unknown error = %v", err)
	}

	models, err := manager.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels at capacity: %v", err)
	}
	if len(models) != 1 || models[0].ID != "composer-test" {
		t.Fatalf("models = %+v", models)
	}
}

func TestCursorAPIKeyErrorInfoOverWire(t *testing.T) {
	manager := newFakeCursorManager(t, Config{APIKey: "bad-key"})
	client := newCursorServiceClient(t, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.CreateSession(ctx, connect.NewRequest(&cursorv1.CreateSessionRequest{}))
	info, ok := fakeServiceErrorInfo(err)
	if !ok || info.GetReason() != cursorAPIKeyErrorReason {
		t.Fatalf("error = %v, ErrorInfo = %+v", err, info)
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeUnauthenticated {
		t.Fatalf("error = %v, want unauthenticated", err)
	}
	if strings.Contains(err.Error(), "bad-key") {
		t.Fatalf("error leaked API key: %v", err)
	}
}

func TestCursorBusyAbortAndIdleReattach(t *testing.T) {
	root := t.TempDir()
	manager := newFakeCursorManager(t, Config{SandboxRoot: root, MaxSessions: 2, IdleTimeout: 30 * time.Millisecond})
	id, err := manager.Create(context.Background(), CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, sendErr := manager.Send(context.Background(), id, "slow", nil)
		result <- sendErr
	}()
	waitFor(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.sessions[id] != nil && manager.sessions[id].runID != ""
	})
	if _, err := manager.Send(context.Background(), id, "second", nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy error = %v", err)
	}
	if err := manager.Abort(context.Background(), id); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	select {
	case sendErr := <-result:
		if sendErr == nil {
			t.Fatal("aborted Send returned nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aborted Send did not return")
	}

	id, err = manager.Create(context.Background(), CreateOpts{})
	if err != nil {
		t.Fatalf("Create for idle: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		_, active := manager.sessions[id]
		return !active
	})
	text, err := manager.Send(context.Background(), id, "after idle", nil)
	if err != nil {
		t.Fatalf("Send after idle: %v", err)
	}
	if text != "echo: after idle" {
		t.Fatalf("text after idle = %q", text)
	}
}

func TestCursorBridgeCrashReturns(t *testing.T) {
	manager := newFakeCursorManager(t, Config{MaxSessions: 1})
	id, err := manager.Create(context.Background(), CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = manager.Send(ctx, id, "crash", nil)
	if err == nil {
		t.Fatal("crashed bridge returned success")
	}
}
