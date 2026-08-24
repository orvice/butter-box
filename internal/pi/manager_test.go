package pi

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCreateSendDelete(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{Name: "test", ThinkingLevel: "low"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if info.ID != fakeSessionID {
		t.Fatalf("session id = %q, want %q", info.ID, fakeSessionID)
	}
	if info.Model != "fake/fake-model" {
		t.Fatalf("model = %q", info.Model)
	}

	var events []string
	result, err := m.Send(ctx, info.ID, "hello", nil, func(eventType string, _ []byte) error {
		events = append(events, eventType)
		return nil
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Text != "echo: hello" {
		t.Fatalf("text = %q", result.Text)
	}
	if result.StopReason != "stop" {
		t.Fatalf("stop reason = %q", result.StopReason)
	}
	if result.Stats.Input != 100 || result.Stats.Output != 25 || result.Stats.ContextPercent != 12 {
		t.Fatalf("stats = %+v", result.Stats)
	}
	if len(events) == 0 || events[len(events)-1] != "agent_settled" {
		t.Fatalf("events = %v, want trailing agent_settled", events)
	}
	if events[0] != "agent_start" {
		t.Fatalf("events = %v, want leading agent_start", events)
	}

	if err := m.Delete(ctx, info.ID, false); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	sessions, err := m.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions after delete = %v", sessions)
	}
}

func TestExtensionDialogAutoCancelled(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The fake blocks the run on an extension confirm dialog; the process
	// layer must auto-answer it or this Send never settles.
	result, err := m.Send(ctx, info.ID, "please dialog with me", nil, nil)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Text != "echo: please dialog with me" {
		t.Fatalf("text = %q", result.Text)
	}
}

func TestSendBusy(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := m.Send(ctx, info.ID, "slow one", nil, func(eventType string, _ []byte) error {
			if eventType == "agent_start" {
				close(started)
			}
			return nil
		})
		done <- err
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first send never started")
	}

	if _, err := m.Send(ctx, info.ID, "second", nil, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent send error = %v, want ErrBusy", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("first send: %v", err)
	}
}

func TestSendCancelAborts(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	sendCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_, err = m.Send(sendCtx, info.ID, "slow run", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Send after cancel = %v, want context.Canceled", err)
	}

	// The abort must have unblocked the fake's run; the session accepts the
	// next message rather than reporting busy.
	if _, err := m.Send(ctx, info.ID, "after abort", nil, nil); err != nil {
		t.Fatalf("Send after abort: %v", err)
	}
}

func TestAttachUnknownSession(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	// The fake, like pi, silently starts a fresh session for an unknown
	// --session value; the manager must detect the ID mismatch.
	if _, _, err := m.Get(ctx, "no-such-session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get unknown = %v, want ErrNotFound", err)
	}
}

func TestReattachKnownSession(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, _, err := m.Get(ctx, "known-abc123")
	if err != nil {
		t.Fatalf("Get known: %v", err)
	}
	if info.ID != "known-abc123" {
		t.Fatalf("session id = %q", info.ID)
	}

	result, err := m.Send(ctx, "known-abc123", "hi again", nil, nil)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Text != "echo: hi again" {
		t.Fatalf("text = %q", result.Text)
	}
}

func TestSubmitAndGetTurn(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// First turn: an empty session has no leaf entry yet.
	cursor, err := m.Submit(ctx, info.ID, "slow async run", nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if cursor != "" {
		t.Fatalf("cursor = %q, want empty for a fresh session", cursor)
	}

	// The run is in flight (the fake's "slow" prompt takes 2s): a pure poll
	// reports running, a second submit is rejected as busy.
	status, err := m.Turn(ctx, info.ID, cursor, 0)
	if err != nil {
		t.Fatalf("Turn poll: %v", err)
	}
	if !status.Running || status.Result != nil {
		t.Fatalf("status mid-run = %+v, want running without result", status)
	}
	if _, err := m.Submit(ctx, info.ID, "second", nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("Submit while running = %v, want ErrBusy", err)
	}

	// Long-poll until it settles.
	status, err = m.Turn(ctx, info.ID, cursor, 10*time.Second)
	if err != nil {
		t.Fatalf("Turn long-poll: %v", err)
	}
	if status.Running || status.Result == nil {
		t.Fatalf("status after settle = %+v, want result", status)
	}
	if status.Result.Text != "echo: slow async run" {
		t.Fatalf("text = %q", status.Result.Text)
	}
	if status.Result.StopReason != "stop" {
		t.Fatalf("stop reason = %q", status.Result.StopReason)
	}
	if status.Result.Stats.Input != 100 {
		t.Fatalf("stats = %+v", status.Result.Stats)
	}

	// Second turn: the cursor now points at the first turn's assistant entry,
	// and the result must be scoped to entries after it.
	cursor2, err := m.Submit(ctx, info.ID, "quick two", nil)
	if err != nil {
		t.Fatalf("Submit second: %v", err)
	}
	if cursor2 == "" || cursor2 == cursor {
		t.Fatalf("cursor2 = %q, want a fresh leaf id", cursor2)
	}
	status, err = m.Turn(ctx, info.ID, cursor2, 10*time.Second)
	if err != nil {
		t.Fatalf("Turn second: %v", err)
	}
	if status.Running || status.Result == nil || status.Result.Text != "echo: quick two" {
		t.Fatalf("second turn status = %+v", status)
	}
}

func TestGetTurnDidNotFinish(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// No run ever produced an assistant message after this cursor: not
	// running, no result — the honest "did not finish" shape.
	status, err := m.Turn(ctx, info.ID, "", 0)
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if status.Running || status.Result != nil {
		t.Fatalf("status = %+v, want neither running nor result", status)
	}
}

func TestGetTurnAfterAbort(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cursor, err := m.Submit(ctx, info.ID, "slow doomed run", nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := m.Abort(ctx, info.ID); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// The aborted run settles without an assistant message: did not finish.
	status, err := m.Turn(ctx, info.ID, cursor, 10*time.Second)
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if status.Running || status.Result != nil {
		t.Fatalf("status after abort = %+v, want neither running nor result", status)
	}
}

func TestGetTurnBadCursor(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := m.Turn(ctx, info.ID, "no-such-entry", 0); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("Turn bad cursor = %v, want ErrBadCursor", err)
	}
}

func TestAvailableModels(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	models, err := m.AvailableModels(ctx, info.ID)
	if err != nil {
		t.Fatalf("AvailableModels: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("models = %+v, want 2", models)
	}
	first := models[0]
	if first.ID != "fake-model" || first.Provider != "fake" || first.Name != "Fake Model" {
		t.Fatalf("first model = %+v", first)
	}
	if first.API != "anthropic-messages" || !first.Reasoning {
		t.Fatalf("first model = %+v", first)
	}
	if len(first.Input) != 2 || first.Input[1] != "image" {
		t.Fatalf("first model input = %v", first.Input)
	}
	if first.ContextWindow != 200000 || first.MaxTokens != 64000 {
		t.Fatalf("first model limits = %+v", first)
	}
	if first.CostInput != 3.0 || first.CostOutput != 15.0 || first.CostCacheRead != 0.3 || first.CostCacheWrite != 3.75 {
		t.Fatalf("first model cost = %+v", first)
	}
	if models[1].ID != "fake-mini" || models[1].Reasoning {
		t.Fatalf("second model = %+v", models[1])
	}

	// Inactive sessions re-attach transparently, like GetSession.
	if err := m.Delete(ctx, info.ID, false); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	models, err = m.AvailableModels(ctx, "known-models-1")
	if err != nil {
		t.Fatalf("AvailableModels after re-attach: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("models after re-attach = %+v, want 2", models)
	}
}

func TestSessionLimit(t *testing.T) {
	t.Setenv("FAKE_PI", "1")
	m := NewManager(testLogger(t), Config{Bin: "pi.test-fake", MaxSessions: 1})
	t.Cleanup(m.Stop)
	ctx := testCtx(t)

	// Seed one active session directly so the limit check trips without
	// depending on spawn timing.
	m.mu.Lock()
	m.sessions["seed"] = &session{id: "seed", runSlot: make(chan struct{}, 1)}
	m.mu.Unlock()

	if _, err := m.Create(ctx, CreateOpts{}); !errors.Is(err, ErrTooManySessions) {
		t.Fatalf("Create over limit = %v, want ErrTooManySessions", err)
	}

	m.mu.Lock()
	delete(m.sessions, "seed")
	m.mu.Unlock()
}
