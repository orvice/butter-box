package pi

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"
	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

// A real Pi provider boundary with no external model, credentials or sessions.
// It issues a real tool call, then answers only after the tool result arrives.
func newHostSmokeClient(t *testing.T) piv1connect.PiServiceClient {
	t.Helper()
	client, _ := newRestartableHostSmokeClient(t)
	return client
}

func newRestartableHostSmokeClient(t *testing.T) (piv1connect.PiServiceClient, func() piv1connect.PiServiceClient) {
	t.Helper()
	if os.Getenv("BUTTERBOX_PI_SMOKE") != "1" {
		t.Skip("set BUTTERBOX_PI_SMOKE=1 for real Pi host relay tests")
	}
	binName := os.Getenv("BUTTERBOX_PI_SMOKE_BIN")
	if binName == "" {
		binName = "pi"
	}
	bin, err := exec.LookPath(binName)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.UnmarshalRead(r.Body, &request); err != nil {
			t.Error(err)
			http.Error(w, "bad request", 400)
			return
		}
		lastUser, results := "", []string{}
		for _, message := range request.Messages {
			if message.Role == "user" {
				lastUser = fmt.Sprint(message.Content)
				results = nil
			}
			if message.Role == "tool" {
				results = append(results, fmt.Sprint(message.Content))
			}
		}
		exposed := false
		for _, tool := range request.Tools {
			if tool.Function.Name == "render_ui" {
				exposed = true
			}
		}
		delta, finish := map[string]any{}, "stop"
		if !exposed && !strings.Contains(lastUser, "force-unnegotiated") {
			delta["content"] = "host tools disabled"
		} else if len(results) == 0 || (len(results) == 1 && strings.Contains(results[0], "fix parameters")) {
			callID, fallback := "card-call-1", "summary"
			if len(results) == 1 {
				callID, fallback = "card-call-2", "corrected summary"
			}
			delta["tool_calls"] = []any{map[string]any{"index": 0, "id": callID, "type": "function", "function": map[string]any{"name": "render_ui", "arguments": `{"messages":[{"updateComponents":{"components":[]}}],"fallback":"` + fallback + `"}`}}}
			finish = "tool_calls"
		} else {
			delta["content"] = "host confirmed: " + results[len(results)-1]
		}
		chunk := func(d any, reason any) {
			body, _ := json.Marshal(map[string]any{"id": "relay-smoke", "object": "chat.completion.chunk", "created": 1, "model": "relay-model", "choices": []any{map[string]any{"index": 0, "delta": d, "finish_reason": reason}}})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		chunk(delta, nil)
		chunk(map[string]any{}, finish)
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(endpoint.Close)
	agentDir, cwd, sessionDir := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("PI_CODING_AGENT_DIR", agentDir)
	t.Setenv("PI_OFFLINE", "1")
	t.Setenv("PI_TELEMETRY", "0")
	t.Setenv("PI_SKIP_VERSION_CHECK", "1")
	models, _ := json.Marshal(map[string]any{"providers": map[string]any{"relay": map[string]any{
		"baseUrl": endpoint.URL + "/v1", "api": "openai-completions", "apiKey": "local-test",
		"models": []any{map[string]any{"id": "relay-model", "name": "Relay model", "reasoning": false, "input": []string{"text"}, "contextWindow": 128000, "maxTokens": 1024, "cost": map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}}},
	}}})
	for name, data := range map[string][]byte{"models.json": models, "settings.json": []byte(`{"defaultProvider":"relay","defaultModel":"relay-model","defaultThinkingLevel":"off"}`)} {
		if err := os.WriteFile(filepath.Join(agentDir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var manager *Manager
	var server *httptest.Server
	start := func() piv1connect.PiServiceClient {
		manager = NewManager(testLogger(t), Config{Bin: bin, SessionDir: sessionDir, SandboxRoot: cwd})
		path, handler := NewServiceHandler(manager)
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		server = httptest.NewServer(mux)
		return piv1connect.NewPiServiceClient(server.Client(), server.URL)
	}
	client := start()
	t.Cleanup(func() { manager.Stop(); server.Close() })
	return client, func() piv1connect.PiServiceClient { manager.Stop(); server.Close(); return start() }
}

func hostConfig() *piv1.HostToolsConfig {
	return &piv1.HostToolsConfig{ProtocolVersion: 1, ContractSha256: hostContractHash(), AllowedTools: []string{"render_ui"}, MaxRunSeconds: 60}
}

func createHostSession(t *testing.T, client piv1connect.PiServiceClient) string {
	t.Helper()
	created, err := client.CreateSession(t.Context(), connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	return created.Msg.GetSession().GetId()
}

func TestRealPiHostRelayCorrectsToolErrorsAndDoesNotLeakAuthorization(t *testing.T) {
	client := newHostSmokeClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	id := createHostSession(t, client)
	capabilities, err := client.GetCapabilities(ctx, connect.NewRequest(&piv1.GetCapabilitiesRequest{SessionId: id}))
	if err != nil || !capabilities.Msg.GetHostToolsExtensionReady() {
		t.Fatalf("live handshake: %v %v", capabilities, err)
	}
	legacy := func(message string) {
		t.Helper()
		run, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: message}))
		if err != nil || run.Msg.GetTurnId() != "" {
			t.Fatalf("legacy run: %v %v", run, err)
		}
		turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.Msg.GetTurnCursor(), WaitSeconds: 10}))
		if err != nil || turn.Msg.GetResult().GetText() != "host tools disabled" {
			t.Fatalf("unnegotiated tools exposed: %v %v", turn, err)
		}
	}
	legacy("before host negotiation")
	run, call := pendingHostCall(t, client, id, "render a summary", hostConfig())
	_, err = client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_Error{Error: "fix parameters: invalid A2UI component"}}))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: run.GetTurnId(), WaitSeconds: 10}))
	if err != nil || len(retry.Msg.GetCalls()) != 1 || retry.Msg.GetCalls()[0].GetCallId() == call.GetCallId() || !strings.Contains(retry.Msg.GetCalls()[0].GetArgumentsJson(), "corrected summary") {
		t.Fatalf("Pi did not correct tool error: %v %v", retry, err)
	}
	receipt := &piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: retry.Msg.GetCalls()[0].GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"surface_id":"corrected","revision":1,"status":"created"}`}}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); err != nil {
		t.Fatal(err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || !strings.Contains(turn.Msg.GetResult().GetText(), `"surface_id":"corrected"`) {
		t.Fatalf("corrected result: %v %v", turn, err)
	}
	legacy("after successful host turn")
	if _, err := client.DeleteSession(ctx, connect.NewRequest(&piv1.DeleteSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
	capabilities, err = client.GetCapabilities(ctx, connect.NewRequest(&piv1.GetCapabilitiesRequest{SessionId: id}))
	if err != nil || !capabilities.Msg.GetHostToolsExtensionReady() {
		t.Fatalf("attach handshake: %v %v", capabilities, err)
	}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("receipt survived process restart: %v", err)
	}
	legacy("after attach")
	// Session history contains render_ui calls, but cannot authorize execution.
	forced, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "force-unnegotiated"}))
	if err != nil {
		t.Fatal(err)
	}
	denied, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: forced.Msg.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || !strings.Contains(strings.ToLower(denied.Msg.GetResult().GetText()), "not") {
		t.Fatalf("historical call was not denied: %v %v", denied, err)
	}
	fresh, freshCall := pendingHostCall(t, client, id, "fresh authorized turn after attach", hostConfig())
	if fresh.GetTurnId() == run.GetTurnId() {
		t.Fatal("restart reused relay identity")
	}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: fresh.GetTurnId(), CallId: freshCall.GetCallId(), Outcome: &piv1.RespondHostCallRequest_Error{Error: "do not render; finish in text"}})); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: fresh.GetTurnCursor(), WaitSeconds: 10})); err != nil {
		t.Fatal(err)
	}
}

func TestRealPiHostRelayAbortAndExpiry(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprint("expiry=", expire), func(t *testing.T) {
			client := newHostSmokeClient(t)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			id := createHostSession(t, client)
			config := hostConfig()
			if expire {
				config.MaxRunSeconds = 2
			}
			run, call := pendingHostCall(t, client, id, "wait for host", config)
			if expire {
				time.Sleep(2200 * time.Millisecond)
			} else {
				if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
					t.Fatal(err)
				}
			}
			_, err := client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"status":"created"}`}}))
			if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition && code != connect.CodeNotFound {
				t.Fatalf("late receipt accepted: %v", err)
			}
			turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10}))
			if err != nil || turn.Msg.GetRunning() || strings.Contains(turn.Msg.GetResult().GetText(), "created") {
				t.Fatalf("interrupted run claimed success: %v %v", turn, err)
			}
			fresh, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "ordinary follow-up"}))
			if err != nil {
				t.Fatal(err)
			}
			turn, err = client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: fresh.Msg.GetTurnCursor(), WaitSeconds: 10}))
			if err != nil || turn.Msg.GetResult().GetText() != "host tools disabled" {
				t.Fatalf("authorization leaked after cancellation: %v %v", turn, err)
			}
		})
	}
}

func TestRealPiHostRelayBoxRestartDiscardsPendingPromises(t *testing.T) {
	client, restart := newRestartableHostSmokeClient(t)
	id := createHostSession(t, client)
	run, call := pendingHostCall(t, client, id, "interrupted host call", hostConfig())
	client = restart()
	_, err := client.RespondHostCall(t.Context(), connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"status":"created"}`}}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("Box restart restored an in-memory promise: %v", err)
	}
	turn, err := client.GetTurn(t.Context(), connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || turn.Msg.GetRunning() || turn.Msg.GetResult() != nil {
		t.Fatalf("interrupted host call claimed completion: %v %v", turn, err)
	}
	next, nextCall := pendingHostCall(t, client, id, "new host call after Box restart", hostConfig())
	if next.GetTurnId() == run.GetTurnId() {
		t.Fatal("Box restart reused turn identity")
	}
	if _, err := client.RespondHostCall(t.Context(), connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: next.GetTurnId(), CallId: nextCall.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"surface_id":"new","revision":1,"status":"created"}`}})); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetTurn(t.Context(), connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: next.GetTurnCursor(), WaitSeconds: 10})); err != nil {
		t.Fatal(err)
	}
}

func TestRealPiHostRelaySuccess(t *testing.T) {
	client := newHostSmokeClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	id := createHostSession(t, client)
	submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "render a summary", HostTools: hostConfig(), ModelSelection: &piv1.ModelSelection{Provider: "relay", ModelId: "relay-model"}}))
	if err != nil {
		t.Fatal(err)
	}
	turnID := submitted.Msg.GetTurnId()
	if turnID == "" {
		t.Fatal("host-enabled submit returned no independent turn identity")
	}
	calls, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: turnID, WaitSeconds: 10}))
	if err != nil || len(calls.Msg.GetCalls()) != 1 || !calls.Msg.GetRunning() {
		t.Fatalf("pending calls = %v, error = %v", calls, err)
	}
	call := calls.Msg.GetCalls()[0]
	if call.GetTool() != "render_ui" || call.GetSessionId() != id || call.GetTurnId() != turnID || !strings.Contains(call.GetArgumentsJson(), `"fallback":"summary"`) {
		t.Fatalf("bad call envelope: %v", call)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor()}))
	if err != nil || !turn.Msg.GetRunning() {
		t.Fatalf("tool must block settlement: %v %v", turn, err)
	}
	_, err = client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: turnID, CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"surface_id":"s1","revision":1,"status":"created"}`}}))
	if err != nil {
		t.Fatal(err)
	}
	turn, err = client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || turn.Msg.GetRunning() || !strings.Contains(turn.Msg.GetResult().GetText(), `"surface_id":"s1"`) {
		t.Fatalf("host result did not reach Pi: %v %v", turn, err)
	}
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id, AfterCursor: submitted.Msg.GetTurnCursor()}))
	if err != nil {
		t.Fatal(err)
	}
	persisted := 0
	for _, entry := range entries.Msg.GetEntries() {
		var data struct {
			Message struct {
				Role       string `json:"role"`
				ToolCallID string `json:"toolCallId"`
				IsError    bool   `json:"isError"`
				Details    struct {
					SurfaceID string `json:"surface_id"`
				} `json:"details"`
			} `json:"message"`
		}
		if err := json.Unmarshal([]byte(entry.GetPayloadJson()), &data); err != nil {
			t.Fatal(err)
		}
		if data.Message.Role == "toolResult" && data.Message.ToolCallID == call.GetCallId() {
			if data.Message.IsError || data.Message.Details.SurfaceID != "s1" {
				t.Fatalf("host result details not preserved: %v", entry)
			}
			persisted++
		}
	}
	if persisted != 1 {
		t.Fatalf("persisted tool results = %d, want exactly one", persisted)
	}
}
