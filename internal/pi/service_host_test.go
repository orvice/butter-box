package pi

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"

	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

func TestHostCallsCanBeRereadAndReceiptsRetried(t *testing.T) {
	t.Setenv("FAKE_PI_HOST", "1")
	client, ctx := newTestClient(t, Config{})
	id := createHostSession(t, client)
	submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: hostConfig()}))
	if err != nil {
		t.Fatal(err)
	}
	read := &piv1.ListHostCallsRequest{SessionId: id, TurnId: submitted.Msg.GetTurnId(), WaitSeconds: 10}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = client.ListHostCalls(cancelled, connect.NewRequest(read))
	first, err := client.ListHostCalls(ctx, connect.NewRequest(read))
	if err != nil || len(first.Msg.GetCalls()) != 1 {
		t.Fatalf("read: %v %v", first, err)
	}
	second, err := client.ListHostCalls(ctx, connect.NewRequest(read))
	if err != nil || len(second.Msg.GetCalls()) != 1 || second.Msg.GetCalls()[0].GetCallId() != first.Msg.GetCalls()[0].GetCallId() {
		t.Fatalf("reread: %v %v", second, err)
	}
	receipt := &piv1.RespondHostCallRequest{SessionId: id, TurnId: read.TurnId, CallId: first.Msg.GetCalls()[0].GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"surface_id":"s1","revision":1}`}}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); err != nil {
		t.Fatal(err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || turn.Msg.GetRunning() || !strings.Contains(turn.Msg.GetResult().GetText(), "s1") {
		t.Fatalf("turn: %v %v", turn, err)
	}
	receipt.Outcome = &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{ "revision": 1, "surface_id": "s1" }`}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); err != nil {
		t.Fatalf("identical retry after settlement: %v", err)
	}
	receipt.Outcome = &piv1.RespondHostCallRequest_Error{Error: "different outcome"}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("conflicting receipt: %v", err)
	}
	// A disconnected read did not cancel the original run; a new ordinary run
	// does not consume any host permission from the completed one.
	next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "legacy next"}))
	if err != nil || next.Msg.GetTurnId() != "" {
		t.Fatalf("legacy submit: %v %v", next, err)
	}
	_, _ = client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10}))
}

func pendingHostCall(t *testing.T, client piv1connect.PiServiceClient, id, message string, config *piv1.HostToolsConfig) (*piv1.SubmitMessageResponse, *piv1.HostCall) {
	t.Helper()
	submitted, err := client.SubmitMessage(t.Context(), connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: message, HostTools: config}))
	if err != nil {
		t.Fatal(err)
	}
	calls, err := client.ListHostCalls(t.Context(), connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: submitted.Msg.GetTurnId(), WaitSeconds: 10}))
	if err != nil || len(calls.Msg.GetCalls()) != 1 {
		t.Fatalf("pending: %v %v", calls, err)
	}
	return submitted.Msg, calls.Msg.GetCalls()[0]
}

func TestHostCallsAreIsolatedAcrossTurnsAndSessions(t *testing.T) {
	t.Setenv("FAKE_PI_HOST", "1")
	client, ctx := newTestClient(t, Config{MaxSessions: 2})
	id := createHostSession(t, client)
	first, call := pendingHostCall(t, client, id, "host request", hostConfig())
	other := "known-other"
	if _, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: other})); err != nil {
		t.Fatal(err)
	}
	second, otherCall := pendingHostCall(t, client, other, "host request", hostConfig())
	if first.GetTurnId() == second.GetTurnId() || call.GetCallId() != otherCall.GetCallId() {
		t.Fatal("fixture must reuse call IDs in independently identified turns")
	}
	receipt := &piv1.RespondHostCallRequest{SessionId: other, TurnId: first.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_Error{Error: "invalid parameters"}}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-session receipt: %v", err)
	}
	receipt.SessionId = id
	receipt.TurnId = second.GetTurnId()
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-turn receipt: %v", err)
	}
	if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
	receipt.TurnId = first.GetTurnId()
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("cancelled receipt: %v", err)
	}
	_, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: first.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil {
		t.Fatal(err)
	}
	next, nextCall := pendingHostCall(t, client, id, "host request", hostConfig())
	if next.GetTurnId() == first.GetTurnId() || nextCall.GetCallId() != call.GetCallId() {
		t.Fatal("later run needs unique turn identity even with identical call IDs")
	}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("late receipt: %v", err)
	}
	pending, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: other, TurnId: second.GetTurnId()}))
	if err != nil || len(pending.Msg.GetCalls()) != 1 {
		t.Fatalf("other session was affected: %v %v", pending, err)
	}
}

func TestHostRelayChecksConfigurationAndLiveReadiness(t *testing.T) {
	for _, mode := range []string{"", "incompatible", "1"} {
		t.Run("extension="+mode, func(t *testing.T) {
			t.Setenv("FAKE_PI_HOST", mode)
			client, ctx := newTestClient(t, Config{})
			id := createHostSession(t, client)
			capabilities, err := client.GetCapabilities(ctx, connect.NewRequest(&piv1.GetCapabilitiesRequest{SessionId: id}))
			if err != nil || capabilities.Msg.GetHostToolsExtensionReady() != (mode == "1") {
				t.Fatalf("readiness: %v %v", capabilities, err)
			}
			if mode == "1" {
				for _, config := range []*piv1.HostToolsConfig{{}, {ProtocolVersion: 99}, {ProtocolVersion: 1, ContractSha256: hostContractHash(), AllowedTools: []string{"bash"}}, {ProtocolVersion: 1, ContractSha256: hostContractHash(), AllowedTools: []string{"render_ui"}, MaxRunSeconds: 86401}} {
					_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: config}))
					if connect.CodeOf(err) != connect.CodeInvalidArgument {
						t.Fatalf("invalid config accepted: %v", err)
					}
				}
			} else {
				_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: hostConfig()}))
				if connect.CodeOf(err) != connect.CodeFailedPrecondition {
					t.Fatalf("missing extension accepted: %v", err)
				}
			}
			sent, err := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{SessionId: id, Message: "legacy still works"}))
			if err != nil || sent.Msg.GetText() != "echo: legacy still works" {
				t.Fatalf("legacy behavior: %v %v", sent, err)
			}
		})
	}
}

func TestHostRelayDeadlineAndDisconnectReleaseCapacity(t *testing.T) {
	for _, message := range []string{"host request", "host disconnect"} {
		t.Run(message, func(t *testing.T) {
			t.Setenv("FAKE_PI_HOST", "1")
			client, ctx := newTestClient(t, Config{MaxSessions: 1})
			id := createHostSession(t, client)
			config := hostConfig()
			config.MaxRunSeconds = 1
			submitted, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: message, HostTools: config}))
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				_, err = client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: "known-replacement"}))
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("lost host retained capacity: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			_, err = client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: submitted.Msg.GetTurnId()}))
			if connect.CodeOf(err) != connect.CodeNotFound {
				t.Fatalf("old process grant survived: %v", err)
			}
		})
	}
}

func TestHostRelayBoundsPayloadsCallsAndReceiptRetention(t *testing.T) {
	t.Setenv("FAKE_PI_HOST", "1")
	client, ctx := newTestClient(t, Config{})
	id := createHostSession(t, client)
	submitted, call := pendingHostCall(t, client, id, "host calls 65", hostConfig())
	for _, outcome := range []*piv1.RespondHostCallRequest{
		{Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `[]`}},
		{Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"invalid":`}},
		{Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"large":"` + strings.Repeat("x", 256*1024) + `"}`}},
		{Outcome: &piv1.RespondHostCallRequest_Error{Error: strings.Repeat("x", 8193)}},
		{},
	} {
		outcome.SessionId, outcome.TurnId, outcome.CallId = id, submitted.GetTurnId(), call.GetCallId()
		if _, err := client.RespondHostCall(ctx, connect.NewRequest(outcome)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("invalid receipt: %v", err)
		}
	}
	for i := 0; i < 64; i++ {
		if i > 0 {
			calls, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: submitted.GetTurnId(), WaitSeconds: 10}))
			if err != nil || len(calls.Msg.GetCalls()) != 1 {
				t.Fatalf("call %d: %v %v", i+1, calls, err)
			}
			call = calls.Msg.GetCalls()[0]
		}
		_, err := client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: submitted.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"revision":9007199254740993}`}}))
		if err != nil {
			t.Fatal(err)
		}
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: submitted.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || !strings.Contains(turn.Msg.GetResult().GetText(), "call limit") {
		t.Fatalf("call limit: %v %v", turn, err)
	}
	// Integer values beyond 2^53 must not collapse to an identical receipt.
	receipt := &piv1.RespondHostCallRequest{SessionId: id, TurnId: submitted.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"revision":9007199254740992}`}}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("precision-losing receipt comparison: %v", err)
	}
	for range 8 {
		run, current := pendingHostCall(t, client, id, "host request", hostConfig())
		receipt = &piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: current.GetCallId(), Outcome: &piv1.RespondHostCallRequest_Error{Error: "ordinary host validation error"}}
		if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); err != nil {
			t.Fatal(err)
		}
		if _, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10})); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.RespondHostCall(ctx, connect.NewRequest(receipt)); err != nil {
		t.Fatalf("latest receipt not retained: %v", err)
	}
	_, err = client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: submitted.GetTurnId()}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("oldest grant not evicted at bounded retention: %v", err)
	}
}

func TestHostRelayRejectsOversizedArgumentsAndParallelBacklog(t *testing.T) {
	for _, message := range []string{"host oversize", "host parallel"} {
		t.Run(message, func(t *testing.T) {
			t.Setenv("FAKE_PI_HOST", "1")
			client, ctx := newTestClient(t, Config{})
			id := createHostSession(t, client)
			run, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: message, HostTools: hostConfig()}))
			if err != nil {
				t.Fatal(err)
			}
			calls, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: run.Msg.GetTurnId(), WaitSeconds: 10}))
			if err != nil {
				t.Fatal(err)
			}
			if message == "host parallel" {
				if len(calls.Msg.GetCalls()) != 1 {
					t.Fatalf("parallel queue: %v", calls)
				}
				_, err = client.RespondHostCall(ctx, connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: id, TurnId: run.Msg.GetTurnId(), CallId: calls.Msg.GetCalls()[0].GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{}`}}))
				if err != nil {
					t.Fatal(err)
				}
			} else if len(calls.Msg.GetCalls()) != 0 {
				t.Fatalf("oversize was queued: %v", calls)
			}
			turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.Msg.GetTurnCursor(), WaitSeconds: 10}))
			if err != nil || turn.Msg.GetRunning() {
				t.Fatalf("bounded work failed to settle: %v %v", turn, err)
			}
			if message == "host oversize" && !strings.Contains(turn.Msg.GetResult().GetText(), "invalid host tool request") {
				t.Fatalf("oversize must return a tool error: %v", turn)
			}
		})
	}
}

func TestHostRelayAbortBeforePromptAcceptanceDoesNotAuthorizeLateRun(t *testing.T) {
	t.Setenv("FAKE_PI_HOST", "1")
	release, wait := fakeGateSetup(t, "FAKE_PI_PROMPT_GATE")
	client, ctx := newTestClient(t, Config{})
	id := createHostSession(t, client)
	completed := make(chan *piv1.SubmitMessageResponse, 1)
	failed := make(chan error, 1)
	go func() {
		run, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: hostConfig()}))
		if err != nil {
			failed <- err
			return
		}
		completed <- run.Msg
	}()
	wait("captured")
	if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
	release("release")
	var run *piv1.SubmitMessageResponse
	select {
	case run = <-completed:
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10})); err != nil {
		t.Fatal(err)
	}
	calls, err := client.ListHostCalls(ctx, connect.NewRequest(&piv1.ListHostCallsRequest{SessionId: id, TurnId: run.GetTurnId()}))
	if err != nil || calls.Msg.GetRunning() || len(calls.Msg.GetCalls()) != 0 {
		t.Fatalf("late run inherited authorization: %v %v", calls, err)
	}
	next, _ := pendingHostCall(t, client, id, "host request", hostConfig())
	if next.GetTurnId() == run.GetTurnId() {
		t.Fatal("turn identity reused")
	}
}

// Drop a real HTTP response after the server has processed the receipt.
type dropHostReceiptResponse struct{ client *http.Client }

func (d dropHostReceiptResponse) Do(req *http.Request) (*http.Response, error) {
	resp, err := d.client.Do(req)
	if err == nil && strings.HasSuffix(req.URL.Path, "/RespondHostCall") {
		_ = resp.Body.Close()
		return nil, errors.New("simulated lost receipt response")
	}
	return resp, err
}

func TestHostReceiptCanBeRetriedAfterHTTPResponseIsLost(t *testing.T) {
	t.Setenv("FAKE_PI", "1")
	t.Setenv("FAKE_PI_HOST", "1")
	manager := NewManager(testLogger(t), Config{Bin: os.Args[0], SessionDir: t.TempDir()})
	t.Cleanup(manager.Stop)
	path, handler := NewServiceHandler(manager)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := piv1connect.NewPiServiceClient(server.Client(), server.URL)
	lossy := piv1connect.NewPiServiceClient(dropHostReceiptResponse{server.Client()}, server.URL)
	id := createHostSession(t, client)
	run, call := pendingHostCall(t, client, id, "host request", hostConfig())
	receipt := &piv1.RespondHostCallRequest{SessionId: id, TurnId: run.GetTurnId(), CallId: call.GetCallId(), Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"surface_id":"once","revision":1,"status":"created"}`}}
	if _, err := lossy.RespondHostCall(t.Context(), connect.NewRequest(receipt)); err == nil {
		t.Fatal("fixture did not lose acknowledgement")
	}
	if _, err := client.RespondHostCall(t.Context(), connect.NewRequest(receipt)); err != nil {
		t.Fatalf("safe retry: %v", err)
	}
	turn, err := client.GetTurn(t.Context(), connect.NewRequest(&piv1.GetTurnRequest{SessionId: id, TurnCursor: run.GetTurnCursor(), WaitSeconds: 10}))
	if err != nil || !strings.Contains(turn.Msg.GetResult().GetText(), "once") {
		t.Fatalf("receipt outcome changed: %v %v", turn, err)
	}
}

func TestHostLongPollDisconnectDoesNotConsumeAndNewCallWakesBeforeSettlement(t *testing.T) {
	t.Setenv("FAKE_PI_HOST", "1")
	release, wait := fakeGateSetup(t, "FAKE_PI_HOST_CALL_GATE")
	client, ctx := newTestClient(t, Config{})
	id := createHostSession(t, client)
	run, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: hostConfig()}))
	if err != nil {
		t.Fatal(err)
	}
	wait("captured")
	request := &piv1.ListHostCallsRequest{SessionId: id, TurnId: run.Msg.GetTurnId(), WaitSeconds: 10}
	readCtx, cancel := context.WithCancel(ctx)
	disconnected := make(chan error, 1)
	go func() { _, err := client.ListHostCalls(readCtx, connect.NewRequest(request)); disconnected <- err }()
	select {
	case err := <-disconnected:
		t.Fatalf("empty long poll returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	if err := <-disconnected; connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("read disconnect: %v", err)
	}
	awakened := make(chan *piv1.ListHostCallsResponse, 1)
	go func() {
		calls, err := client.ListHostCalls(ctx, connect.NewRequest(request))
		if err != nil {
			awakened <- nil
			return
		}
		awakened <- calls.Msg
	}()
	release("release")
	select {
	case calls := <-awakened:
		if calls == nil || len(calls.GetCalls()) != 1 || !calls.GetRunning() {
			t.Fatalf("pending call not rereadable before settlement: %v", calls)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new pending call did not wake long poll")
	}
	if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
}

func TestHostDeadlineSurvivesUnconfirmedAbort(t *testing.T) {
	for _, preflight := range []bool{false, true} {
		t.Run(fmt.Sprint("preflight=", preflight), func(t *testing.T) {
			t.Setenv("FAKE_PI_HOST", "1")
			_, abortWaiting := fakeGateSetup(t, "FAKE_PI_ABORT_GATE")
			var promptWaiting func(string)
			if preflight {
				var release func(string)
				release, promptWaiting = fakeGateSetup(t, "FAKE_PI_PROMPT_GATE")
				defer release("release")
			}
			client, ctx := newTestClient(t, Config{MaxSessions: 1})
			id := createHostSession(t, client)
			config := hostConfig()
			config.MaxRunSeconds = 1
			if preflight {
				done := make(chan error, 1)
				go func() {
					_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "host request", HostTools: config}))
					done <- err
				}()
				promptWaiting("captured")
			} else {
				_, _ = pendingHostCall(t, client, id, "host request", config)
			}
			abortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			_, err := client.AbortSession(abortCtx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id}))
			if err == nil {
				t.Fatal("fixture confirmed abort unexpectedly")
			}
			abortWaiting("captured")
			deadline := time.Now().Add(3 * time.Second)
			for {
				_, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: "known-after-unconfirmed-abort"}))
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("unconfirmed abort disabled host deadline: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestHostReceiptIngressIsBoundedBeforeDecoding(t *testing.T) {
	manager := NewManager(testLogger(t), Config{Bin: "/missing-pi"})
	t.Cleanup(manager.Stop)
	path, handler := NewServiceHandler(manager)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, options := range [][]connect.ClientOption{nil, {connect.WithProtoJSON()}, {connect.WithSendGzip()}, {connect.WithProtoJSON(), connect.WithSendGzip()}} {
		client := piv1connect.NewPiServiceClient(server.Client(), server.URL, options...)
		_, err := client.RespondHostCall(t.Context(), connect.NewRequest(&piv1.RespondHostCallRequest{SessionId: "missing", TurnId: "missing", CallId: "missing", Outcome: &piv1.RespondHostCallRequest_ResultJson{ResultJson: `{"large":"` + strings.Repeat("x", 2*1024*1024) + `"}`}}))
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("oversized encoded/decompressed receipt reached handler: %v", err)
		}
	}
}

func TestHostToolsDiscoveryDoesNotClaimExtensionReadiness(t *testing.T) {
	manager := NewManager(testLogger(t), Config{Bin: "/missing-pi"})
	t.Cleanup(manager.Stop)
	path, handler := NewServiceHandler(manager)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path+"GetCapabilities", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var capabilities struct {
		Version uint32 `json:"hostToolsProtocolVersion"`
		Hash    string `json:"hostToolsContractSha256"`
		Ready   bool   `json:"hostToolsExtensionReady"`
	}
	if err := json.UnmarshalRead(resp.Body, &capabilities); err != nil {
		t.Fatal(err)
	}
	if capabilities.Version != 1 || len(capabilities.Hash) != 64 || capabilities.Ready {
		t.Fatalf("capabilities = %+v; want v1 contract without process readiness", capabilities)
	}
}
