package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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

func TestEntries(t *testing.T) {
	m := newFakeManager(t)
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// A fresh session has no entries.
	res, err := m.Entries(ctx, info.ID, "")
	if err != nil {
		t.Fatalf("Entries empty: %v", err)
	}
	if len(res.Entries) != 0 || res.LeafID != "" || res.Running {
		t.Fatalf("empty session entries = %+v", res)
	}

	if _, err := m.Send(ctx, info.ID, "one", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Full transcript: the user and assistant message entries, oldest first.
	res, err = m.Entries(ctx, info.ID, "")
	if err != nil {
		t.Fatalf("Entries full: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %+v, want 2", res.Entries)
	}
	for _, e := range res.Entries {
		if e.Type != "message" || e.ID == "" || !json.Valid(e.Raw) {
			t.Fatalf("entry = %+v, want valid message entry", e)
		}
	}
	if !strings.Contains(string(res.Entries[1].Raw), "echo: one") {
		t.Fatalf("assistant entry = %s, want echoed text", res.Entries[1].Raw)
	}
	if res.LeafID != res.Entries[1].ID {
		t.Fatalf("leaf = %q, want %q", res.LeafID, res.Entries[1].ID)
	}
	if res.Running {
		t.Fatalf("running after settle, want false")
	}

	// Incremental poll: only entries past the cursor.
	tail, err := m.Entries(ctx, info.ID, res.Entries[0].ID)
	if err != nil {
		t.Fatalf("Entries after cursor: %v", err)
	}
	if len(tail.Entries) != 1 || tail.Entries[0].ID != res.LeafID {
		t.Fatalf("tail entries = %+v, want just the leaf", tail.Entries)
	}

	if _, err := m.Entries(ctx, info.ID, "no-such-entry"); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("Entries bad cursor = %v, want ErrBadCursor", err)
	}

	// Mid-run a listing reports running so a refresh loop keeps polling.
	cursor, err := m.Submit(ctx, info.ID, "slow two", nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	mid, err := m.Entries(ctx, info.ID, cursor)
	if err != nil {
		t.Fatalf("Entries mid-run: %v", err)
	}
	if !mid.Running {
		t.Fatalf("mid-run entries = %+v, want running", mid)
	}
	if _, err := m.Turn(ctx, info.ID, cursor, 10*time.Second); err != nil {
		t.Fatalf("Turn settle: %v", err)
	}
	done, err := m.Entries(ctx, info.ID, cursor)
	if err != nil {
		t.Fatalf("Entries after settle: %v", err)
	}
	if len(done.Entries) != 2 || done.Running {
		t.Fatalf("settled entries = %+v, want 2 new entries and not running", done)
	}
}

func TestListIncludesDiskSessions(t *testing.T) {
	sessionDir := t.TempDir()
	sub := filepath.Join(sessionDir, "--proj--")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	writeSession := func(path, id, cwd string, age time.Duration) {
		t.Helper()
		header := fmt.Sprintf(`{"type":"session","version":3,"id":%q,"timestamp":"2026-01-01T00:00:00.000Z","cwd":%q}`, id, cwd)
		if err := os.WriteFile(path, []byte(header+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		mtime := time.Now().Add(-age)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	writeSession(filepath.Join(sessionDir, "a_disk-old.jsonl"), "disk-old", "/old/dir", 2*time.Hour)
	newFile := filepath.Join(sub, "b_disk-new.jsonl")
	writeSession(newFile, "disk-new", "/new/dir", time.Hour)
	// A stale duplicate of disk-new: the newest file must win.
	writeSession(filepath.Join(sessionDir, "c_disk-new.jsonl"), "disk-new", "/stale/dir", 3*time.Hour)
	// A disk file for the active session must not produce a duplicate row.
	writeSession(filepath.Join(sessionDir, "d_active.jsonl"), fakeSessionID, "/active/dir", time.Minute)
	// Non-session files are ignored.
	if err := os.WriteFile(filepath.Join(sessionDir, "junk.jsonl"), []byte(`{"type":"other"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SessionDir: sessionDir})
	ctx := testCtx(t)

	if _, err := m.Create(ctx, CreateOpts{}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	infos, err := m.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var ids []string
	for _, info := range infos {
		ids = append(ids, info.ID)
	}
	// Newest first; the active session's reported file does not exist on
	// disk, so its mtime is unknown and it sorts last.
	want := []string{"disk-new", "disk-old", fakeSessionID}
	if !slices.Equal(ids, want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}

	byID := map[string]Info{}
	for _, info := range infos {
		byID[info.ID] = info
	}
	if !byID[fakeSessionID].Active {
		t.Fatalf("active session = %+v, want Active", byID[fakeSessionID])
	}
	diskNew := byID["disk-new"]
	if diskNew.Active || diskNew.Cwd != "/new/dir" || diskNew.File != newFile || diskNew.UpdatedAt.IsZero() {
		t.Fatalf("disk-new = %+v", diskNew)
	}
	if byID["disk-old"].Cwd != "/old/dir" {
		t.Fatalf("disk-old = %+v", byID["disk-old"])
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

func TestBoxAvailableModels(t *testing.T) {
	readSpawns := spawnLogSetup(t)
	m := newFakeManagerCfg(t, Config{MaxSessions: 4})
	ctx := testCtx(t)

	models, err := m.AvailableModels(ctx, "")
	if err != nil {
		t.Fatalf("AvailableModels(session-less): %v", err)
	}
	if len(models) != 2 || models[0].ID != "fake-model" {
		t.Fatalf("models = %+v, want the 2-model catalog", models)
	}

	// One transient, session-less process answered the query.
	spawns := readSpawns()
	if len(spawns) != 1 {
		t.Fatalf("spawns = %+v, want 1", spawns)
	}
	if !slices.Contains(spawns[0].Args, "--no-session") {
		t.Fatalf("spawn args = %v, want --no-session", spawns[0].Args)
	}

	// The catalog is cached, so a repeated query spawns nothing.
	if _, err := m.AvailableModels(ctx, ""); err != nil {
		t.Fatalf("AvailableModels(cached): %v", err)
	}
	if spawns := readSpawns(); len(spawns) != 1 {
		t.Fatalf("spawns after cached query = %+v, want 1", spawns)
	}

	// An expired catalog is fetched again.
	m.mu.Lock()
	m.catalogAt = time.Now().Add(-2 * modelCatalogTTL)
	m.mu.Unlock()
	if _, err := m.AvailableModels(ctx, ""); err != nil {
		t.Fatalf("AvailableModels(expired): %v", err)
	}
	if spawns := readSpawns(); len(spawns) != 2 {
		t.Fatalf("spawns after expiry = %+v, want 2", spawns)
	}
}

// TestBoxAvailableModelsAtSessionLimit covers the capacity case: the transient
// catalog process is outside the MaxSessions accounting, so a box at its
// session cap still answers.
func TestBoxAvailableModelsAtSessionLimit(t *testing.T) {
	m := newFakeManagerCfg(t, Config{MaxSessions: 1})
	ctx := testCtx(t)

	if _, err := m.Create(ctx, CreateOpts{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := m.Create(ctx, CreateOpts{}); !errors.Is(err, ErrTooManySessions) {
		t.Fatalf("Create over limit = %v, want ErrTooManySessions", err)
	}

	models, err := m.AvailableModels(ctx, "")
	if err != nil {
		t.Fatalf("AvailableModels at session limit: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("models = %+v, want 2", models)
	}
}

// TestBoxAvailableModelsServesStale covers a failing spawn: the catalog barely
// changes, so the last known answer beats no answer at all.
func TestBoxAvailableModelsServesStale(t *testing.T) {
	m := newFakeManagerCfg(t, Config{MaxSessions: 4})
	ctx := testCtx(t)

	if _, err := m.AvailableModels(ctx, ""); err != nil {
		t.Fatalf("AvailableModels: %v", err)
	}

	// Age out the cache and break the pi binary: the stale catalog answers.
	m.mu.Lock()
	m.catalogAt = time.Now().Add(-2 * modelCatalogTTL)
	m.mu.Unlock()
	m.cfg.Bin = filepath.Join(t.TempDir(), "no-such-pi")

	models, err := m.AvailableModels(ctx, "")
	if err != nil {
		t.Fatalf("AvailableModels with broken pi = %v, want the stale catalog", err)
	}
	if len(models) != 2 {
		t.Fatalf("stale models = %+v, want 2", models)
	}
}

func TestBoxAvailableModelsError(t *testing.T) {
	m := newFakeManagerCfg(t, Config{MaxSessions: 4})
	m.cfg.Bin = filepath.Join(t.TempDir(), "no-such-pi")

	// Nothing cached and no process to ask: the error must surface.
	if _, err := m.AvailableModels(testCtx(t), ""); err == nil {
		t.Fatal("AvailableModels with broken pi and empty cache succeeded, want error")
	}
}

// sameDir compares paths after resolving symlinks (macOS tempdirs live under
// /var -> /private/var, so the child's os.Getwd differs textually).
func sameDir(t *testing.T, got, want string) {
	t.Helper()
	gotReal, err := filepath.EvalSymlinks(got)
	if err != nil {
		t.Fatalf("resolve %q: %v", got, err)
	}
	wantReal, err := filepath.EvalSymlinks(want)
	if err != nil {
		t.Fatalf("resolve %q: %v", want, err)
	}
	if gotReal != wantReal {
		t.Fatalf("dir = %q, want %q", gotReal, wantReal)
	}
}

func TestCreateSpawnsInRequestedCwd(t *testing.T) {
	root := t.TempDir()
	proj := filepath.Join(root, "proj")
	if err := os.Mkdir(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	readSpawns := spawnLogSetup(t)
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SandboxRoot: root})
	ctx := testCtx(t)

	info, err := m.Create(ctx, CreateOpts{Cwd: "proj"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sameDir(t, info.Cwd, proj)

	spawns := readSpawns()
	if len(spawns) != 1 {
		t.Fatalf("spawns = %v, want 1", spawns)
	}
	sameDir(t, spawns[0].Cwd, proj)
}

func TestCreateCwdValidation(t *testing.T) {
	root := t.TempDir()
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SandboxRoot: root})
	ctx := testCtx(t)

	for _, cwd := range []string{"../escape", "/etc", "does-not-exist"} {
		if _, err := m.Create(ctx, CreateOpts{Cwd: cwd}); !errors.Is(err, ErrInvalidCwd) {
			t.Fatalf("Create(cwd=%q) = %v, want ErrInvalidCwd", cwd, err)
		}
	}

	// A cwd pointing at a file (not a directory) is rejected too.
	file := filepath.Join(root, "afile")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(ctx, CreateOpts{Cwd: "afile"}); !errors.Is(err, ErrInvalidCwd) {
		t.Fatalf("Create(cwd=file) = %v, want ErrInvalidCwd", err)
	}
}

func TestReattachSpawnsInSessionCwd(t *testing.T) {
	sessionDir := t.TempDir()
	workDir := t.TempDir()

	// pi records the session cwd in the session file header; the manager must
	// read it back so a re-attached process spawns in the session's own cwd.
	// The file sits in a per-cwd subdirectory like pi's default layout.
	subDir := filepath.Join(sessionDir, "--encoded-cwd--")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	header := fmt.Sprintf(`{"type":"session","version":3,"id":"known-abc123","timestamp":"2026-01-01T00:00:00.000Z","cwd":%q}`, workDir)
	sessionFile := filepath.Join(subDir, "2026-01-01T00-00-00-000Z_known-abc123.jsonl")
	if err := os.WriteFile(sessionFile, []byte(header+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	readSpawns := spawnLogSetup(t)
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SessionDir: sessionDir})
	ctx := testCtx(t)

	info, _, err := m.Get(ctx, "known-abc123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if info.Cwd != workDir {
		t.Fatalf("info.Cwd = %q, want %q", info.Cwd, workDir)
	}

	spawns := readSpawns()
	if len(spawns) != 1 {
		t.Fatalf("spawns = %v, want 1", spawns)
	}
	sameDir(t, spawns[0].Cwd, workDir)
}

func TestFindSessionCwd(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "x_flat-id.jsonl"),
		[]byte(`{"type":"session","id":"flat-id","cwd":"/flat/dir"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A file whose name matches but whose header belongs to another session
	// must be ignored.
	if err := os.WriteFile(filepath.Join(root, "y_other-id.jsonl"),
		[]byte(`{"type":"session","id":"different","cwd":"/wrong"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := findSessionCwd(root, "flat-id"); got != "/flat/dir" {
		t.Fatalf("findSessionCwd = %q, want /flat/dir", got)
	}
	if got := findSessionCwd(root, "other-id"); got != "" {
		t.Fatalf("findSessionCwd mismatched header = %q, want empty", got)
	}
	if got := findSessionCwd(root, "missing"); got != "" {
		t.Fatalf("findSessionCwd missing = %q, want empty", got)
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
