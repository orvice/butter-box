package pi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
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
	manager := NewManager(testLogger(t), Config{Bin: os.Args[0], MaxSessions: 4})
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

	if _, err := client.DeleteSession(ctx, connect.NewRequest(&piv1.DeleteSessionRequest{SessionId: id})); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
}

func TestServiceErrorCodes(t *testing.T) {
	t.Setenv("FAKE_PI", "1")
	manager := NewManager(testLogger(t), Config{Bin: os.Args[0], MaxSessions: 4})
	t.Cleanup(manager.Stop)

	path, handler := piv1connect.NewPiServiceHandler(NewService(manager))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := piv1connect.NewPiServiceClient(srv.Client(), srv.URL)
	ctx := testCtx(t)

	_, err := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{
		SessionId: "no-such-session",
		Message:   "hi",
	}))
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeNotFound {
		t.Fatalf("error = %v, want CodeNotFound", err)
	}
}
