package pi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	piv1 "github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1"
	"github.com/orvice/butter-box/pkg/proto/butterbox/pi/v1/piv1connect"
)

func TestSubmitMessageSerializesModelSetupAndDetachedRun(t *testing.T) {
	releaseModel, waitModel := fakeGateSetup(t, "FAKE_PI_MODEL_GATE")
	finishRun, waitRun := fakeGateSetup(t, "FAKE_PI_ENTRIES_GATE")
	commands := commandLogSetup(t)
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	type outcome struct {
		response *connect.Response[piv1.SubmitMessageResponse]
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
			SessionId: id, Message: "gated run",
			ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
		}))
		done <- outcome{response, err}
	}()
	waitModel("captured")
	rejectCompetingSends(t, client, ctx, id)
	assertCommandCount(t, commands(), "set_model", 1)
	assertCommandCount(t, commands(), "prompt", 0)
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
	if err != nil || !entries.Msg.GetRunning() || len(entries.Msg.GetEntries()) != 0 {
		t.Fatalf("setup entries = %v, error = %v; want running without a prompt", entries, err)
	}
	releaseModel("release")
	var submitted *connect.Response[piv1.SubmitMessageResponse]
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		submitted = result.response
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitRun("started")
	// Returning a cursor must not release the slot while the run is detached.
	rejectCompetingSends(t, client, ctx, id)
	assertCommandCount(t, commands(), "set_model", 1)
	assertCommandCount(t, commands(), "prompt", 1)
	finishRun("finish")
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: submitted.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetResult().GetText() != "echo: gated run" {
		t.Fatalf("turn = %v, error = %v", turn, err)
	}
}

func TestSubmitMessageCannotSwitchModelDuringSendOrStream(t *testing.T) {
	for _, entryPoint := range []string{"send", "stream"} {
		t.Run(entryPoint, func(t *testing.T) {
			finish, wait := fakeGateSetup(t, "FAKE_PI_ENTRIES_GATE")
			commands := commandLogSetup(t)
			client, ctx := newTestClient(t, Config{MaxSessions: 4})
			created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			id := created.Msg.GetSession().GetId()
			done := make(chan error, 1)
			go func() {
				if entryPoint == "send" {
					_, err := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{SessionId: id, Message: "gated run"}))
					done <- err
					return
				}
				stream, err := client.StreamMessage(ctx, connect.NewRequest(&piv1.StreamMessageRequest{SessionId: id, Message: "gated run"}))
				if err == nil {
					for stream.Receive() {
					}
					err = stream.Err()
				}
				done <- err
			}()
			wait("started")
			_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
				SessionId: id, Message: "cannot switch",
				ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
			}))
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || modelErrorReason(t, err) != "" {
				t.Fatalf("busy error = %v, must retain existing busy semantics", err)
			}
			assertCommandCount(t, commands(), "set_model", 0)
			finish("finish")
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestSubmitMessageDisconnectDuringModelSetupDoesNotAbort(t *testing.T) {
	release, wait := fakeGateSetup(t, "FAKE_PI_MODEL_GATE")
	finish, waitRun := fakeGateSetup(t, "FAKE_PI_ENTRIES_GATE")
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	submitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.SubmitMessage(submitCtx, connect.NewRequest(&piv1.SubmitMessageRequest{
			SessionId: id, Message: "gated run",
			ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
		}))
		done <- err
	}()
	wait("captured")
	cancel()
	if err := <-done; connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("disconnected request = %v", err)
	}
	release("release")
	waitRun("started")
	finish("finish")
	// The first entry is the model change; it is also the lost turn cursor.
	entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
	if err != nil || len(entries.Msg.GetEntries()) == 0 {
		t.Fatalf("entries = %v, error = %v", entries, err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: entries.Msg.GetEntries()[0].GetId(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetResult().GetText() != "echo: gated run" {
		t.Fatalf("detached turn = %v, error = %v", turn, err)
	}
}

func TestAbortSessionDuringModelSetupPreventsPrompt(t *testing.T) {
	release, wait := fakeGateSetup(t, "FAKE_PI_MODEL_GATE")
	commands := commandLogSetup(t)
	client, ctx := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetSession().GetId()
	done := make(chan error, 1)
	go func() {
		_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
			SessionId: id, Message: "cancel before prompt",
			ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
		}))
		done <- err
	}()
	wait("captured")
	if _, err := client.AbortSession(ctx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id})); err != nil {
		t.Fatal(err)
	}
	release("release")
	if err := <-done; connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("aborted model setup = %v", err)
	}
	assertCommandCount(t, commands(), "prompt", 0)
	next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: id, Message: "next reservation"}))
	if err != nil || next.Msg.GetAppliedModel().GetModelId() != "fake-mini" {
		t.Fatalf("next submit = %v, error = %v; no rollback or stale abort intent", next, err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetResult().GetText() != "echo: next reservation" {
		t.Fatalf("next turn = %v, error = %v", turn, err)
	}
}

func TestAbortSessionCannotEscapeIntoNextReservation(t *testing.T) {
	for _, disconnectAbort := range []bool{false, true} {
		name := "acknowledged abort"
		if disconnectAbort {
			name = "disconnected abort"
		}
		t.Run(name, func(t *testing.T) {
			releaseModel, waitModel := fakeGateSetup(t, "FAKE_PI_MODEL_GATE")
			releaseAbort, waitAbort := fakeGateSetup(t, "FAKE_PI_ABORT_GATE")
			commands := commandLogSetup(t)
			client, ctx := newTestClient(t, Config{MaxSessions: 4})
			created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			id := created.Msg.GetSession().GetId()
			submitDone := make(chan error, 1)
			go func() {
				_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
					SessionId: id, Message: "cancelled reservation",
					ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
				}))
				submitDone <- err
			}()
			waitModel("captured")
			abortCtx, cancelAbort := context.WithCancel(ctx)
			defer cancelAbort()
			abortDone := make(chan error, 1)
			go func() {
				_, err := client.AbortSession(abortCtx, connect.NewRequest(&piv1.AbortSessionRequest{SessionId: id}))
				abortDone <- err
			}()
			waitAbort("captured")
			if disconnectAbort {
				cancelAbort()
				if err := <-abortDone; connect.CodeOf(err) != connect.CodeCanceled {
					t.Fatalf("disconnected abort = %v", err)
				}
			}
			releaseModel("release")
			if err := <-submitDone; connect.CodeOf(err) != connect.CodeCanceled {
				t.Fatalf("first reservation = %v", err)
			}
			// The first prompt was suppressed, but its native abort is not resolved.
			// Do not let a new reservation start and be hit by that old abort.
			_, err = client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
				SessionId: id, Message: "slow next reservation",
				ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-model"},
			}))
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || modelErrorReason(t, err) != "" {
				t.Fatalf("unresolved abort must keep sends busy: %v", err)
			}
			assertCommandCount(t, commands(), "prompt", 0)
			releaseAbort("release")
			if !disconnectAbort {
				if err := <-abortDone; err != nil {
					t.Fatal(err)
				}
			}
			for {
				entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: id}))
				if err != nil {
					t.Fatal(err)
				}
				if !entries.Msg.GetRunning() {
					break
				}
				select {
				case <-time.After(time.Millisecond):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
				SessionId: id, Message: "slow next reservation",
				ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-model"},
			}))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
				SessionId: id, TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
			}))
			if err != nil || turn.Msg.GetResult().GetText() != "echo: slow next reservation" {
				t.Fatalf("old abort cancelled next reservation: %v, error = %v", turn, err)
			}
		})
	}
}

func TestSubmitMessageModelTimeoutCannotChangeNextRun(t *testing.T) {
	// Use the actual production setup deadline: cancellation of the request
	// must not change this timeout or abort a detached setup.
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	gate := t.TempDir()
	t.Setenv("FAKE_PI_MODEL_GATE", gate)
	runGate := t.TempDir()
	t.Setenv("FAKE_PI_ENTRIES_GATE", runGate)
	spawns := spawnLogSetup(t)
	client, _ := newTestClient(t, Config{MaxSessions: 4})
	created, err := client.CreateSession(ctx, connect.NewRequest(&piv1.CreateSessionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	original := created.Msg.GetSession()
	done := make(chan error, 1)
	go func() {
		_, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
			SessionId: original.GetId(), Message: "must not prompt after timeout",
			ModelSelection: &piv1.ModelSelection{Provider: "fake", ModelId: "fake-mini"},
		}))
		done <- err
	}()
	if err := waitForFakeFile(ctx, filepath.Join(gate, "captured")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Fatalf("setup timeout = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A timed-out waiter is not cancellation of the pending native command.
	// All send entry points must be quarantined until that command resolves.
	_, submitErr := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: original.GetId(), Message: "must not interleave"}))
	_, sendErr := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{SessionId: original.GetId(), Message: "must not interleave"}))
	stream, streamErr := client.StreamMessage(ctx, connect.NewRequest(&piv1.StreamMessageRequest{SessionId: original.GetId(), Message: "must not interleave"}))
	if streamErr == nil {
		for stream.Receive() {
		}
		streamErr = stream.Err()
	}
	for _, err := range []error{submitErr, sendErr, streamErr} {
		if modelErrorReason(t, err) != "PI_MODEL_SET_FAILED" {
			t.Fatalf("unresolved model setup must reject sends: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(gate, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		entries, err := client.ListEntries(ctx, connect.NewRequest(&piv1.ListEntriesRequest{SessionId: original.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		if !entries.Msg.GetRunning() {
			if len(entries.Msg.GetEntries()) != 1 || entries.Msg.GetEntries()[0].GetType() != "model_change" {
				t.Fatalf("entries = %v, timed-out setup must never send its prompt", entries.Msg)
			}
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	next, err := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{SessionId: original.GetId(), Message: "gated run"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := waitForFakeFile(ctx, filepath.Join(runGate, "started")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runGate, "finish"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	turn, err := client.GetTurn(ctx, connect.NewRequest(&piv1.GetTurnRequest{
		SessionId: original.GetId(), TurnCursor: next.Msg.GetTurnCursor(), WaitSeconds: 10,
	}))
	if err != nil || turn.Msg.GetResult().GetText() != "echo: gated run" {
		t.Fatalf("next turn = %v, error = %v", turn, err)
	}
	got, err := client.GetSession(ctx, connect.NewRequest(&piv1.GetSessionRequest{SessionId: original.GetId()}))
	if err != nil || got.Msg.GetSession().GetModel() != "fake/fake-mini" || got.Msg.GetSession().GetId() != original.GetId() || got.Msg.GetSession().GetSessionFile() != original.GetSessionFile() || got.Msg.GetSession().GetCwd() != original.GetCwd() {
		t.Fatalf("session continuity after delayed set_model = %v, error = %v", got, err)
	}
	if records := spawns(); len(records) != 1 {
		t.Fatalf("spawns = %v, must retain the original (possibly not yet persisted) session", records)
	}
}

func rejectCompetingSends(t *testing.T, client piv1connect.PiServiceClient, ctx context.Context, id string) {
	t.Helper()
	_, submitErr := client.SubmitMessage(ctx, connect.NewRequest(&piv1.SubmitMessageRequest{
		SessionId: id, Message: "competing submit",
		ModelSelection: &piv1.ModelSelection{Provider: "other", ModelId: "fake-mini"},
	}))
	_, sendErr := client.SendMessage(ctx, connect.NewRequest(&piv1.SendMessageRequest{SessionId: id, Message: "competing send"}))
	stream, streamErr := client.StreamMessage(ctx, connect.NewRequest(&piv1.StreamMessageRequest{SessionId: id, Message: "competing stream"}))
	if streamErr == nil {
		for stream.Receive() {
		}
		streamErr = stream.Err()
	}
	for _, err := range []error{submitErr, sendErr, streamErr} {
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || modelErrorReason(t, err) != "" {
			t.Fatalf("competing send = %v, want existing busy error", err)
		}
	}
}

func assertCommandCount(t *testing.T, commands []string, command string, want int) {
	t.Helper()
	count := 0
	for _, name := range commands {
		if name == command {
			count++
		}
	}
	if count != want {
		t.Fatalf("%s commands = %d, want %d; protocol = %v", command, count, want, commands)
	}
}
