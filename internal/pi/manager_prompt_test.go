package pi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSendCancellationBeforePromptAcknowledgementKeepsRunSerialized(t *testing.T) {
	for _, observeEvents := range []bool{false, true} {
		t.Run(map[bool]string{false: "unary", true: "stream"}[observeEvents], func(t *testing.T) {
			release, wait := fakeGateSetup(t, "FAKE_PI_PROMPT_GATE")
			commands := commandLogSetup(t)
			manager := newFakeManager(t)
			ctx := testCtx(t)
			original, err := manager.Create(ctx, CreateOpts{})
			if err != nil {
				t.Fatal(err)
			}
			var onEvent func(string, []byte) error
			if observeEvents {
				onEvent = func(string, []byte) error { return nil }
			}
			sendCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := manager.Send(sendCtx, original.ID, "slow cancelled preflight", nil, onEvent)
				done <- err
			}()
			wait("captured")
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled send = %v", err)
			}
			_, err = manager.SubmitWithModel(ctx, original.ID, "cannot interleave", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
			if !errors.Is(err, ErrBusy) {
				t.Fatalf("unacknowledged prompt must keep session busy: %v", err)
			}
			assertCommandCount(t, commands(), "set_model", 0)
			release("release")
			turn, err := manager.Turn(ctx, original.ID, "", 5*time.Second)
			if err != nil || turn.Running || turn.Result != nil {
				t.Fatalf("cancelled preflight turn = %+v, error = %v", turn, err)
			}
			assertCommandCount(t, commands(), "abort", 1)
			next, err := manager.SubmitWithModel(ctx, original.ID, "after cancellation", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
			if err != nil {
				t.Fatal(err)
			}
			turn, err = manager.Turn(ctx, original.ID, next.Cursor, 5*time.Second)
			if err != nil || turn.Result == nil || turn.Result.Text != "echo: after cancellation" {
				t.Fatalf("next turn = %+v, error = %v", turn, err)
			}
		})
	}
}

func TestAbortSessionBeforePromptAcknowledgementCancelsLateAcceptedTurn(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		name := "before deadline"
		if timedOut {
			name = "after timeout"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			gate := t.TempDir()
			t.Setenv("FAKE_PI_PROMPT_GATE", gate)
			manager := newFakeManager(t)
			original, err := manager.Create(ctx, CreateOpts{})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := manager.SubmitWithModel(ctx, original.ID, "slow explicitly cancelled preflight", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
				done <- err
			}()
			if err := waitForFakeFile(ctx, filepath.Join(gate, "captured")); err != nil {
				t.Fatal(err)
			}
			if timedOut {
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("prompt acknowledgement timeout = %v", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			// Pi is idle during preflight and can acknowledge this abort now.
			// ButterBox must remember it when the pending prompt is accepted.
			if err := manager.Abort(ctx, original.ID); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(gate, "release"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if !timedOut {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			turn, err := manager.Turn(ctx, original.ID, "", 5*time.Second)
			if err != nil || turn.Running || turn.Result != nil {
				t.Fatalf("explicitly cancelled late turn = %+v, error = %v", turn, err)
			}
			info, _, err := manager.Get(ctx, original.ID)
			if err != nil || info.ID != original.ID || info.File != original.File || info.Cwd != original.Cwd || info.Model != "fake/fake-mini" {
				t.Fatalf("cancelled session continuity = %+v, error = %v", info, err)
			}
			next, err := manager.SubmitWithModel(ctx, original.ID, "after explicit cancellation", nil, &ModelSelection{Provider: "fake", ModelID: "fake-model"})
			if err != nil {
				t.Fatal(err)
			}
			turn, err = manager.Turn(ctx, original.ID, next.Cursor, 5*time.Second)
			if err != nil || turn.Result == nil || turn.Result.Text != "echo: after explicit cancellation" {
				t.Fatalf("next turn = %+v, error = %v", turn, err)
			}
		})
	}
}

func TestSendUnconfirmedAbortKeepsRunSerialized(t *testing.T) {
	t.Setenv("FAKE_PI_ABORT_ERROR", "1")
	finish, wait := fakeGateSetup(t, "FAKE_PI_ENTRIES_GATE")
	manager := newFakeManager(t)
	ctx := testCtx(t)
	original, err := manager.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatal(err)
	}
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := manager.Send(sendCtx, original.ID, "gated run", nil, nil)
		done <- err
	}()
	wait("started")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled send = %v", err)
	}
	_, err = manager.SubmitWithModel(ctx, original.ID, "cannot switch after failed abort", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("unconfirmed abort must keep session busy: %v", err)
	}
	finish("finish")
	turn, err := manager.Turn(ctx, original.ID, "", 5*time.Second)
	if err != nil || turn.Running || turn.Result == nil || turn.Result.Text != "echo: gated run" {
		t.Fatalf("unaborted run = %+v, error = %v", turn, err)
	}
	next, err := manager.SubmitWithModel(ctx, original.ID, "after settlement", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Turn(ctx, original.ID, next.Cursor, 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestSendCancelledPreflightCanResolveWithoutStartingRun(t *testing.T) {
	for _, prompt := range []string{"handled prompt", "late reject prompt"} {
		t.Run(prompt, func(t *testing.T) {
			release, wait := fakeGateSetup(t, "FAKE_PI_PROMPT_GATE")
			commands := commandLogSetup(t)
			manager := newFakeManager(t)
			ctx := testCtx(t)
			original, err := manager.Create(ctx, CreateOpts{})
			if err != nil {
				t.Fatal(err)
			}
			sendCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := manager.Send(sendCtx, original.ID, prompt, nil, nil)
				done <- err
			}()
			wait("captured")
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			release("release")
			turn, err := manager.Turn(ctx, original.ID, "", 5*time.Second)
			if err != nil || turn.Running || turn.Result != nil {
				t.Fatalf("unstarted turn = %+v, error = %v", turn, err)
			}
			assertCommandCount(t, commands(), "abort", 0)
			next, err := manager.SubmitWithModel(ctx, original.ID, "after preflight", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Turn(ctx, original.ID, next.Cursor, 5*time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHandledPromptDoesNotWaitForSettlementOrReturnStaleText(t *testing.T) {
	manager := newFakeManager(t)
	ctx := testCtx(t)
	original, err := manager.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Send(ctx, original.ID, "previous answer", nil, nil); err != nil {
		t.Fatal(err)
	}
	result, err := manager.Send(ctx, original.ID, "handled prompt", nil, nil)
	if err != nil || result.Text != "" {
		t.Fatalf("handled prompt = %+v, error = %v; must not wait or return the previous answer", result, err)
	}
	submitted, err := manager.SubmitWithModel(ctx, original.ID, "handled prompt", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := manager.Turn(ctx, original.ID, submitted.Cursor, 0)
	if err != nil || turn.Running || turn.Result != nil {
		t.Fatalf("handled submit turn = %+v, error = %v", turn, err)
	}
}

func TestSubmitPromptTimeoutKeepsModelUntilLateAcceptedRunSettles(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	promptGate, runGate := t.TempDir(), t.TempDir()
	t.Setenv("FAKE_PI_PROMPT_GATE", promptGate)
	t.Setenv("FAKE_PI_ENTRIES_GATE", runGate)
	commands := commandLogSetup(t)
	manager := newFakeManager(t)
	original, err := manager.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := manager.SubmitWithModel(ctx, original.ID, "gated run", nil, &ModelSelection{Provider: "fake", ModelID: "fake-mini"})
		done <- err
	}()
	if err := waitForFakeFile(ctx, filepath.Join(promptGate, "captured")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("prompt acknowledgement timeout = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	competing := func() {
		t.Helper()
		_, err := manager.SubmitWithModel(ctx, original.ID, "cannot change accepted run", nil, &ModelSelection{Provider: "fake", ModelID: "fake-model"})
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("uncertain or accepted prompt must stay busy: %v", err)
		}
		assertCommandCount(t, commands(), "set_model", 1)
		assertCommandCount(t, commands(), "prompt", 1)
	}
	competing()
	if err := os.WriteFile(filepath.Join(promptGate, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForFakeFile(ctx, filepath.Join(runGate, "started")); err != nil {
		t.Fatal(err)
	}
	// A late success response is not settlement. Keep the slot for the run.
	competing()
	if err := os.WriteFile(filepath.Join(runGate, "finish"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	turn, err := manager.Turn(ctx, original.ID, "", 5*time.Second)
	if err != nil || turn.Running || turn.Result == nil || turn.Result.Text != "echo: gated run" {
		t.Fatalf("late accepted turn = %+v, error = %v", turn, err)
	}
	info, _, err := manager.Get(ctx, original.ID)
	if err != nil || info.Model != "fake/fake-mini" || info.ID != original.ID || info.File != original.File || info.Cwd != original.Cwd {
		t.Fatalf("late accepted turn lost its model or session: %+v, error = %v", info, err)
	}
	next, err := manager.SubmitWithModel(ctx, original.ID, "back to A", nil, &ModelSelection{Provider: "fake", ModelID: "fake-model"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Turn(ctx, original.ID, next.Cursor, 5*time.Second); err != nil {
		t.Fatal(err)
	}
}
