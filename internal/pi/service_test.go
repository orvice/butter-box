package pi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"
	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

// TestServiceEndToEnd exercises the generated ConnectRPC surface against the
// fake pi: create, get, unary send, streaming send, delete.
func TestServiceEndToEnd(t *testing.T) {
	t.Setenv("FAKE_PI", "1")
	manager := NewManager(testLogger(t), Config{Bin: os.Args[0], MaxSessions: 4, SessionDir: t.TempDir()})
	t.Cleanup(manager.Stop)

	path, handler := piv1connect.NewPiServiceHandler(NewService(manager))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := piv1connect.NewPiServiceClient(srv.Client(), srv.URL)
	ctx := testCtx(t)

	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{Name: "e2e"}))
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	id := created.Msg.GetSession().GetId()
	if id != fakeSessionID {
		t.Fatalf("session id = %q", id)
	}

	got, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: id}))
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Msg.GetStats().GetInputTokens() != 100 {
		t.Fatalf("stats = %+v", got.Msg.GetStats())
	}
	if !got.Msg.GetSession().GetActive() {
		t.Fatalf("session = %+v, want active", got.Msg.GetSession())
	}

	models, err := client.GetAvailableModels(ctx, connect.NewRequest(&piv1.GetAvailableModelsRequest{SessionId: id}))
	if err != nil {
		t.Fatalf("GetAvailableModels: %v", err)
	}
	if len(models.Msg.GetModels()) != 2 {
		t.Fatalf("models = %+v, want 2", models.Msg.GetModels())
	}
	first := models.Msg.GetModels()[0]
	if first.GetId() != "fake-model" || first.GetCost().GetOutput() != 15.0 {
		t.Fatalf("first model = %+v", first)
	}

	sent, err := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{
		SessionId: id,
		Message:   "hello over connect",
	}))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.Msg.GetText() != "echo: hello over connect" {
		t.Fatalf("text = %q", sent.Msg.GetText())
	}
	if sent.Msg.GetStopReason() != "stop" {
		t.Fatalf("stop reason = %q", sent.Msg.GetStopReason())
	}

	// The transcript so far: user and assistant message entries.
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	all := entries.Msg.GetEntries()
	if len(all) != 2 || all[0].GetType() != "message" {
		t.Fatalf("entries = %+v, want 2 message entries", all)
	}
	if !strings.Contains(all[1].GetPayloadJson(), "echo: hello over connect") {
		t.Fatalf("assistant entry = %q", all[1].GetPayloadJson())
	}
	if entries.Msg.GetLeafId() != all[1].GetId() || entries.Msg.GetRunning() {
		t.Fatalf("entries listing = %+v, want leaf on last entry and not running", entries.Msg)
	}

	// Incremental poll from a cursor returns just the newer entries.
	tail, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{
		SessionId:   id,
		AfterCursor: all[0].GetId(),
	}))
	if err != nil {
		t.Fatalf("ListEntries after cursor: %v", err)
	}
	if len(tail.Msg.GetEntries()) != 1 || tail.Msg.GetEntries()[0].GetId() != entries.Msg.GetLeafId() {
		t.Fatalf("tail = %+v, want just the leaf entry", tail.Msg.GetEntries())
	}

	stream, err := client.StreamMessage(ctx, connect.NewRequest(&piv1.StreamMessageRequest{
		SessionId: id,
		Message:   "stream me",
	}))
	if err != nil {
		t.Fatalf("StreamMessage: %v", err)
	}
	var types []string
	var sawDelta bool
	for stream.Receive() {
		msg := stream.Msg()
		types = append(types, msg.GetType())
		if msg.GetType() == "message_update" && strings.Contains(msg.GetPayloadJson(), "stream me") {
			sawDelta = true
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(types) == 0 || types[len(types)-1] != "agent_settled" {
		t.Fatalf("stream types = %v, want trailing agent_settled", types)
	}
	if !sawDelta {
		t.Fatalf("stream types = %v, missing text delta payload", types)
	}

	// Async path: submit, observe running via poll, then long-poll the result.
	submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id,
		Message:   "slow async hello",
	}))
	if err != nil {
		t.Fatalf("SubmitMessage: %v", err)
	}
	cursor := submitted.Msg.GetTurnCursor()

	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId:  id,
		TurnCursor: cursor,
	}))
	if err != nil {
		t.Fatalf("GetTurn poll: %v", err)
	}
	if !turn.Msg.GetRunning() {
		t.Fatalf("turn = %+v, want running", turn.Msg)
	}

	turn, err = client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId:   id,
		TurnCursor:  cursor,
		WaitSeconds: 10,
	}))
	if err != nil {
		t.Fatalf("GetTurn long-poll: %v", err)
	}
	if turn.Msg.GetRunning() || turn.Msg.GetResult() == nil {
		t.Fatalf("turn after settle = %+v, want result", turn.Msg)
	}
	if turn.Msg.GetResult().GetText() != "echo: slow async hello" {
		t.Fatalf("turn text = %q", turn.Msg.GetResult().GetText())
	}
	if turn.Msg.GetResult().GetStopReason() != "stop" {
		t.Fatalf("turn stop reason = %q", turn.Msg.GetResult().GetStopReason())
	}

	if _, err := client.DeleteSession(ctx, connect.NewRequest(&piv1.DeleteSessionRequest{SessionId: id})); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
}

// TestServiceBoxCatalogAndDirectories exercises the two session-less RPCs a
// dashboard needs before any agent session exists.
func TestServiceBoxCatalogAndDirectories(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"repo-a", "repo-b/sub", ".git"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	client, ctx := newTestClient(t, Config{MaxSessions: 4, SandboxRoot: root})

	// No session_id: the catalog comes from the box itself.
	models, err := client.GetAvailableModels(ctx, connect.NewRequest(&piv1.GetAvailableModelsRequest{}))
	if err != nil {
		t.Fatalf("GetAvailableModels(session-less): %v", err)
	}
	if len(models.Msg.GetModels()) != 2 || models.Msg.GetModels()[0].GetId() != "fake-model" {
		t.Fatalf("models = %+v", models.Msg.GetModels())
	}

	listed, err := client.ListDirectories(ctx, connect.NewRequest(&piv1.ListDirectoriesRequest{}))
	if err != nil {
		t.Fatalf("ListDirectories(root): %v", err)
	}
	dirs := listed.Msg.GetDirectories()
	if len(dirs) != 2 || dirs[0].GetName() != "repo-a" || dirs[1].GetName() != "repo-b" {
		t.Fatalf("directories = %+v, want repo-a and repo-b", dirs)
	}
	if listed.Msg.GetTruncated() {
		t.Fatalf("listing truncated, want false")
	}

	// The returned path walks down one level and is usable as a session cwd.
	listed, err = client.ListDirectories(ctx, connect.NewRequest(&piv1.ListDirectoriesRequest{
		Path: dirs[1].GetPath(),
	}))
	if err != nil {
		t.Fatalf("ListDirectories(repo-b): %v", err)
	}
	if len(listed.Msg.GetDirectories()) != 1 || listed.Msg.GetDirectories()[0].GetName() != "sub" {
		t.Fatalf("repo-b listing = %+v, want [sub]", listed.Msg.GetDirectories())
	}

	hidden, err := client.ListDirectories(ctx, connect.NewRequest(&piv1.ListDirectoriesRequest{
		IncludeHidden: true,
	}))
	if err != nil {
		t.Fatalf("ListDirectories(include_hidden): %v", err)
	}
	if len(hidden.Msg.GetDirectories()) != 3 {
		t.Fatalf("hidden listing = %+v, want 3", hidden.Msg.GetDirectories())
	}
}

func TestServiceErrorCodes(t *testing.T) {
	client, ctx := newTestClient(t, Config{MaxSessions: 4, SandboxRoot: t.TempDir()})

	_, err := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{
		SessionId: "no-such-session",
		Message:   "hi",
	}))
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeNotFound {
		t.Fatalf("error = %v, want CodeNotFound", err)
	}

	// A directory escaping the sandbox root is an invalid argument, like cwd.
	_, err = client.ListDirectories(ctx, connect.NewRequest(&piv1.ListDirectoriesRequest{
		Path: "../escape",
	}))
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("ListDirectories escape error = %v, want CodeInvalidArgument", err)
	}
}

// newTestClient serves the pi service over HTTP against the fake pi and
// returns a client for it.
func newTestClient(t *testing.T, cfg Config) (piv1connect.PiServiceClient, context.Context) {
	t.Helper()
	manager := newFakeManagerCfg(t, cfg)

	path, handler := piv1connect.NewPiServiceHandler(NewService(manager))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return piv1connect.NewPiServiceClient(srv.Client(), srv.URL), testCtx(t)
}
