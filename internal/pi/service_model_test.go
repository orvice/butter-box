package pi

import (
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"
	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

func TestPiCapabilitiesDiscoverableBeforeSubmission(t *testing.T) {
	// No Pi executable, credentials, or capacity are needed for discovery.
	manager := NewManager(testLogger(t), Config{Bin: "/missing-pi"})
	t.Cleanup(manager.Stop)
	path, handler := piv1connect.NewPiServiceHandler(NewService(manager))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	url := srv.URL + "/butterbox.pi.v1.PiService/GetCapabilities"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("capability discovery status = %d, want 200", resp.StatusCode)
	}
	var capabilities struct {
		SubmitMessageModelSelection bool `json:"submitMessageModelSelection"`
	}
	if err := json.UnmarshalRead(resp.Body, &capabilities); err != nil {
		t.Fatal(err)
	}
	if !capabilities.SubmitMessageModelSelection {
		t.Fatal("model selection capability missing")
	}
}

func TestPiCapabilitiesDistinguishesLegacyBox(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"unknown RPC", ""},
		{"absent capability", "{}"},
		{"disabled capability", `{"submitMessageModelSelection":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.body == "" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)
			client := piv1connect.NewPiServiceClient(srv.Client(), srv.URL, connect.WithProtoJSON())
			capabilities, err := client.GetCapabilities(t.Context(), connect.NewRequest(&piv1.GetCapabilitiesRequest{}))
			if err == nil && capabilities.Msg.GetSubmitMessageModelSelection() {
				t.Fatal("legacy Box must not advertise safe model selection")
			}
		})
	}
}

func TestSubmitMessagePreservesHistoryAndExactModelIdentity(t *testing.T) {
	root := t.TempDir()
	client, ctx := newTestClient(t, Config{MaxSessions: 4, SandboxRoot: root})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{Cwd: root}))
	if err != nil {
		t.Fatal(err)
	}
	original := created.Msg.GetSession()
	var previous []*piv1.Entry
	for _, tc := range []struct {
		message   string
		selection *piv1.ModelSelection
		want      string
	}{
		{"legacy first", nil, "fake/fake-model"},
		{"switch to B", &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"}, "fake/fake-mini"},
		{"legacy keeps B", nil, "fake/fake-mini"},
		{"same ID other provider", &piv1.ModelSelection{Provider: "other", ModelId: "fake-mini"}, "other/fake-mini"},
		{"back to A", &piv1.ModelSelection{Provider: "fake", ModelId: "fake-model"}, "fake/fake-model"},
	} {
		submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
			SessionId: original.GetId(), Message: tc.message, ModelSelection: tc.selection,
		}))
		if err != nil {
			t.Fatal(err)
		}
		applied := submitted.Msg.GetAppliedModel()
		if applied.GetProvider()+"/"+applied.GetModelId() != tc.want {
			t.Fatalf("%s: applied = %v, want %s", tc.message, applied, tc.want)
		}
		turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
			SessionId: original.GetId(), TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10,
		}))
		if err != nil || turn.Msg.GetResult().GetText() != "echo: "+tc.message {
			t.Fatalf("%s: turn = %v, error = %v", tc.message, turn, err)
		}
		got, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: original.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		session := got.Msg.GetSession()
		if session.GetId() != original.GetId() || session.GetSessionFile() != original.GetSessionFile() || session.GetCwd() != filepath.Clean(root) || session.GetModel() != tc.want {
			t.Fatalf("%s: session continuity lost: %v", tc.message, session)
		}
		entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: original.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		all := entries.Msg.GetEntries()
		if len(all) <= len(previous) {
			t.Fatalf("%s: transcript did not grow", tc.message)
		}
		for i, entry := range previous {
			if entry.GetPayloadJson() != all[i].GetPayloadJson() || entry.GetId() != all[i].GetId() {
				t.Fatalf("%s: previous entry %d was changed", tc.message, i)
			}
		}
		if identity := assistantIdentity(t, all[len(all)-1]); identity != tc.want {
			t.Fatalf("%s: assistant used %s, want %s", tc.message, identity, tc.want)
		}
		previous = all
	}
}

func TestSubmitMessagePromptFailureDoesNotRollbackOrRetry(t *testing.T) {
	commands := commandLogSetup(t)
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id, Message: "reject prompt",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if connect.CodeOf(err) != connect.CodeInternal || !strings.Contains(err.Error(), "prompt rejected") {
		t.Fatalf("prompt failure = %v", err)
	}
	assertCommandCount(t, commands(), "prompt", 1)
	got, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: id}))
	if err != nil || got.Msg.GetSession().GetModel() != "fake/fake-mini" {
		t.Fatalf("model after prompt failure = %v, error = %v; no rollback", got, err)
	}
	next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "after rejection"}))
	if err != nil {
		t.Fatalf("slot after prompt failure: %v", err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetResult().GetText() != "echo: after rejection" {
		t.Fatalf("turn = %v, error = %v", turn, err)
	}
}

func TestSubmitMessageSelectedTurnCanBeAborted(t *testing.T) {
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id, Message: "slow cancelled turn",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetRunning() || turn.Msg.GetResult() != nil {
		t.Fatalf("aborted turn = %v, error = %v; want settled without a stale result", turn, err)
	}
	next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "after cancellation"}))
	if err != nil || next.Msg.GetAppliedModel().GetModelId() != "fake-mini" {
		t.Fatalf("next submit = %v, error = %v; want reusable slot and current model", next, err)
	}
	if _, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
	})); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitMessageSelectionPreservesActivationErrors(t *testing.T) {
	client, ctx := newTestClient(t, Config{MaxSessions: 1})
	_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: "missing", Message: "cannot attach",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("missing session = %v", err)
	}
	if _, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{})); err != nil {
		t.Fatal(err)
	}
	_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: "known-another", Message: "cannot attach at capacity",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("at capacity = %v", err)
	}
}

func assistantIdentity(t *testing.T, entry *piv1.Entry) string {
	t.Helper()
	var assistant struct {
		Message struct {
			Provider string `json:"provider"`
			Model    string `json:"model"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(entry.GetPayloadJson()), &assistant); err != nil {
		t.Fatal(err)
	}
	return assistant.Message.Provider + "/" + assistant.Message.Model
}

func TestSubmitMessageModelErrorsDoNotPrompt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider string
		modelID  string
		code     connect.Code
		reason   string
	}{
		{"empty selection", "", "", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"missing provider", "", "fake-mini", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"missing model", "fake", "", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"blank provider", " ", "fake-mini", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"unknown model", "fake", "missing", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"fuzzy model", "fake", "mini", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"case mismatch", "Fake", "fake-mini", connect.CodeInvalidArgument, "PI_MODEL_INVALID"},
		{"auth unavailable", "unauthenticated", "fake-mini", connect.CodeUnauthenticated, "PI_MODEL_AUTH_UNAVAILABLE"},
		{"set failed", "broken", "fake-mini", connect.CodeFailedPrecondition, "PI_MODEL_SET_FAILED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, ctx := newTestClient(t, Config{MaxSessions: 4})
			created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			id := created.Msg.GetSession().GetId()
			_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
				SessionId: id, Message: "must not send",
				ModelSelection: &piv1.ModelSelection{Provider: tc.provider, ModelId: tc.modelID},
			}))
			if connect.CodeOf(err) != tc.code || modelErrorReason(t, err) != tc.reason {
				t.Fatalf("error = %v, want %v / %s", err, tc.code, tc.reason)
			}
			entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
			if err != nil || len(entries.Msg.GetEntries()) != 0 || entries.Msg.GetRunning() {
				t.Fatalf("entries after failure = %v, error = %v; want idle, no prompt", entries, err)
			}
			// The failed setup must release the slot for another explicit selection.
			next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
				SessionId: id, Message: "after failure",
				ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
			}))
			if err != nil {
				t.Fatalf("slot after failure: %v", err)
			}
			turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
				SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
			}))
			if err != nil || turn.Msg.GetResult().GetText() != "echo: after failure" {
				t.Fatalf("next turn = %v, error = %v", turn, err)
			}
		})
	}
}

func TestSubmitMessageRejectsUnappliedSelection(t *testing.T) {
	t.Setenv("FAKE_PI_SET_MODEL_NOOP", "1")
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id, Message: "must not execute on A",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if modelErrorReason(t, err) != "PI_MODEL_SET_FAILED" {
		t.Fatalf("error = %v, want failure when Pi acknowledges B but still reports A", err)
	}
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
	if err != nil || len(entries.Msg.GetEntries()) != 0 {
		t.Fatalf("entries = %v, error = %v; unapplied selection must not send a prompt", entries, err)
	}
}

func modelErrorReason(t *testing.T, err error) string {
	t.Helper()
	connectErr, ok := errors.AsType[*connect.Error](err)
	if !ok {
		return ""
	}
	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err != nil {
			t.Fatal(err)
		}
		if info, ok := value.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func TestSubmitMessageSelectsFirstTurnModel(t *testing.T) {
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id, Message: "first on mini",
		ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	applied := submitted.Msg.GetAppliedModel()
	if applied.GetProvider() != "fake" || applied.GetModelId() != "fake-mini" {
		t.Fatalf("applied model = %v, want fake/fake-mini", applied)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetRunning() || turn.Msg.GetResult().GetText() != "echo: first on mini" {
		t.Fatalf("turn = %v, error = %v", turn, err)
	}
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
	if err != nil {
		t.Fatal(err)
	}
	all := entries.Msg.GetEntries()
	if len(all) != 3 || all[0].GetType() != "model_change" || submitted.Msg.GetTurnCursor() != all[0].GetId() {
		t.Fatalf("entries = %v, cursor = %q; want model change before turn cursor", all, submitted.Msg.GetTurnCursor())
	}
	if identity := assistantIdentity(t, all[2]); identity != "fake/fake-mini" {
		t.Fatalf("assistant identity = %s, want fake/fake-mini", identity)
	}
}
