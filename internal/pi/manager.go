package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Sentinel errors the service layer maps to RPC codes.
var (
	ErrBusy            = errors.New("session is processing another message")
	ErrTooManySessions = errors.New("active session limit reached")
	ErrNotFound        = errors.New("session not found")
	ErrBadCursor       = errors.New("unknown turn cursor")
	ErrInvalidCwd      = errors.New("invalid working directory")
	ErrInvalidPath     = errors.New("invalid path")
)

const (
	defaultMaxSessions  = 8
	defaultIdleTimeout  = 30 * time.Minute
	defaultSetupTimeout = 60 * time.Second
	janitorInterval     = time.Minute
	// maxTurnWait caps GetTurn long-polling so no single request outlives
	// typical proxy idle timeouts.
	maxTurnWait = 30 * time.Second
	// modelCatalogTTL bounds how long the session-less model catalog is served
	// from cache: it only changes when the box's pi config changes, so
	// repeated dropdown loads must not spawn a process each time.
	modelCatalogTTL = 5 * time.Minute
)

// Config configures the session manager.
type Config struct {
	// Bin is the pi executable. Defaults to "pi".
	Bin string
	// MaxSessions caps concurrently active pi processes.
	MaxSessions int
	// IdleTimeout stops a session process after inactivity. The session file
	// stays on disk and the session re-attaches on next use.
	IdleTimeout time.Duration
	// SessionDir overrides pi's session storage directory when set.
	SessionDir string
	// SandboxRoot bounds caller-supplied session working directories, using
	// the same rules as the MCP tools. Empty rejects every cwd request.
	SandboxRoot string
}

func (c Config) withDefaults() Config {
	if c.Bin == "" {
		c.Bin = "pi"
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = defaultMaxSessions
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	return c
}

// CreateOpts are the caller-selectable knobs for a new session.
type CreateOpts struct {
	Name          string
	Provider      string
	Model         string
	ThinkingLevel string
	// Cwd is the working directory for the pi process, absolute or relative
	// to the sandbox root. Empty keeps the server process working directory.
	Cwd string
}

// Info is a session state snapshot.
type Info struct {
	ID           string
	Name         string
	File         string
	Model        string
	Streaming    bool
	MessageCount int32
	Cwd          string
}

// Stats is a cumulative session usage snapshot.
type Stats struct {
	Input          int64
	Output         int64
	CacheRead      int64
	CacheWrite     int64
	Cost           float64
	ContextPercent int32
}

// Model describes one model available to a pi process.
type Model struct {
	ID            string
	Provider      string
	Name          string
	API           string
	Reasoning     bool
	Input         []string
	ContextWindow int64
	MaxTokens     int64
	// Costs are USD per million tokens.
	CostInput      float64
	CostOutput     float64
	CostCacheRead  float64
	CostCacheWrite float64
}

// SendResult is the outcome of one settled prompt run.
type SendResult struct {
	Text       string
	StopReason string
	Stats      Stats
}

type session struct {
	id       string
	file     string
	cwd      string
	proc     *process
	runSlot  chan struct{} // capacity 1: holding the token = running a prompt
	turn     *turnState    // guarded by Manager.mu; non-nil while a submitted run is in flight
	lastUsed time.Time
}

// turnState tracks one detached (submitted) run.
type turnState struct {
	done chan struct{} // closed when the run settles or the process exits
}

// TurnStatus is the answer to one GetTurn poll.
type TurnStatus struct {
	Running bool
	// Result is set when the turn produced an assistant message. Nil with
	// Running=false means the turn did not finish.
	Result *SendResult
}

// Manager owns the pi session processes on this box.
type Manager struct {
	cfg    Config
	logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	inflight map[string]chan struct{} // attach singleflight, keyed by session ID
	cwds     map[string]string        // session ID -> cwd, survives idle-stop
	stopped  bool

	// Session-less model catalog, guarded by mu; catalogGate (capacity 1)
	// serializes the transient spawns that fill it.
	catalog     []Model
	catalogAt   time.Time
	catalogGate chan struct{}

	janitorStop chan struct{}
}

func NewManager(logger *slog.Logger, cfg Config) *Manager {
	m := &Manager{
		cfg:         cfg.withDefaults(),
		logger:      logger,
		sessions:    map[string]*session{},
		inflight:    map[string]chan struct{}{},
		cwds:        map[string]string{},
		catalogGate: make(chan struct{}, 1),
		janitorStop: make(chan struct{}),
	}
	go m.janitor()
	return m
}

// Stop terminates every session process.
func (m *Manager) Stop() {
	close(m.janitorStop)
	m.mu.Lock()
	m.stopped = true
	procs := make([]*process, 0, len(m.sessions))
	for id, s := range m.sessions {
		procs = append(procs, s.proc)
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	for _, p := range procs {
		p.stop()
	}
}

// Create spawns a fresh pi session.
func (m *Manager) Create(ctx context.Context, opts CreateOpts) (Info, error) {
	spawnDir := ""
	if opts.Cwd != "" {
		var err error
		spawnDir, err = m.resolveCwd(opts.Cwd)
		if err != nil {
			return Info{}, err
		}
	}
	cwd := spawnDir
	if cwd == "" {
		// Report the effective directory even when the caller left it default.
		cwd, _ = os.Getwd()
	}

	args := []string{"--mode", "rpc"}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}
	if opts.Provider != "" {
		args = append(args, "--provider", opts.Provider)
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	args = m.appendSessionDir(args)

	proc, err := m.spawn(args, spawnDir)
	if err != nil {
		return Info{}, err
	}

	setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()

	if opts.ThinkingLevel != "" {
		if _, err := proc.callOK(setupCtx, map[string]any{"type": "set_thinking_level", "level": opts.ThinkingLevel}); err != nil {
			proc.stop()
			return Info{}, err
		}
	}

	state, err := getState(setupCtx, proc)
	if err != nil {
		proc.stop()
		return Info{}, err
	}
	if state.SessionID == "" {
		proc.stop()
		return Info{}, errors.New("pi reported no session id")
	}

	s := &session{
		id:       state.SessionID,
		file:     state.SessionFile,
		cwd:      cwd,
		proc:     proc,
		runSlot:  make(chan struct{}, 1),
		lastUsed: time.Now(),
	}

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		proc.stop()
		return Info{}, errors.New("manager stopped")
	}
	if _, ok := m.sessions[s.id]; ok {
		// pi should never mint a duplicate ID; keep the existing process.
		m.mu.Unlock()
		proc.stop()
		return Info{}, fmt.Errorf("session %s already exists", s.id)
	}
	m.sessions[s.id] = s
	m.cwds[s.id] = cwd
	m.mu.Unlock()

	m.logger.Info("pi session created",
		slog.String("session_id", s.id),
		slog.String("session_file", s.file),
		slog.String("cwd", cwd),
	)
	return infoWithCwd(state, cwd), nil
}

// resolveCwd validates a caller-supplied working directory: it must resolve
// inside the sandbox root (same rules as the MCP tools) and exist.
func (m *Manager) resolveCwd(userPath string) (string, error) {
	resolved, err := m.resolveDir(userPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidCwd, err)
	}
	return resolved, nil
}

// List reports currently active sessions.
func (m *Manager) List(ctx context.Context) ([]Info, error) {
	m.mu.Lock()
	active := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		active = append(active, s)
	}
	m.mu.Unlock()

	infos := make([]Info, 0, len(active))
	for _, s := range active {
		state, err := getState(ctx, s.proc)
		if err != nil {
			// Process may be mid-shutdown; report what we know.
			infos = append(infos, Info{ID: s.id, File: s.file, Cwd: s.cwd})
			continue
		}
		infos = append(infos, infoWithCwd(state, s.cwd))
	}
	return infos, nil
}

// Get reports state and stats for one session, re-attaching if needed.
func (m *Manager) Get(ctx context.Context, id string) (Info, Stats, error) {
	s, err := m.attach(ctx, id)
	if err != nil {
		return Info{}, Stats{}, err
	}
	state, err := getState(ctx, s.proc)
	if err != nil {
		return Info{}, Stats{}, err
	}
	stats, err := getStats(ctx, s.proc)
	if err != nil {
		return Info{}, Stats{}, err
	}
	m.touch(s)
	return infoWithCwd(state, s.cwd), stats, nil
}

// Send delivers one prompt and blocks until the run settles. onEvent, when
// non-nil, receives every agent event in order; an onEvent error aborts the
// run and is returned.
func (m *Manager) Send(ctx context.Context, id, message string, images []ImageInput, onEvent func(eventType string, payload []byte) error) (SendResult, error) {
	s, err := m.attach(ctx, id)
	if err != nil {
		return SendResult{}, err
	}

	select {
	case s.runSlot <- struct{}{}:
	default:
		return SendResult{}, ErrBusy
	}
	defer func() {
		<-s.runSlot
		m.touch(s)
	}()

	sub := s.proc.subscribe()
	defer sub.Cancel()

	cmd := map[string]any{"type": "prompt", "message": message}
	if len(images) > 0 {
		payload := make([]imagePayload, len(images))
		for i, img := range images {
			payload[i] = imagePayload{Type: "image", Data: img.Base64Data, MimeType: img.MimeType}
		}
		cmd["images"] = payload
	}
	if _, err := s.proc.callOK(ctx, cmd); err != nil {
		return SendResult{}, err
	}

	stopReason, err := m.waitSettled(ctx, s.proc, sub, onEvent)
	if err != nil {
		return SendResult{}, err
	}

	text, err := getLastAssistantText(ctx, s.proc)
	if err != nil {
		return SendResult{}, err
	}
	stats, err := getStats(ctx, s.proc)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{Text: text, StopReason: stopReason, Stats: stats}, nil
}

// Submit delivers one prompt and returns immediately with the turn cursor:
// pi's entries cursor (leaf entry id) at submit time, stable across process
// restarts. The run is detached from ctx — cancelling the request never
// aborts it; Abort is the only way to cancel. Await the result with Turn.
func (m *Manager) Submit(ctx context.Context, id, message string, images []ImageInput) (string, error) {
	s, err := m.attach(ctx, id)
	if err != nil {
		return "", err
	}

	select {
	case s.runSlot <- struct{}{}:
	default:
		return "", ErrBusy
	}
	release := func() {
		<-s.runSlot
		m.touch(s)
	}

	// The setup calls run on their own timeout, not the request context: once
	// the RPC has reached the server, a client disconnect must not leave a
	// half-started turn behind.
	setupCtx, cancel := context.WithTimeout(context.Background(), defaultSetupTimeout)
	defer cancel()

	cursor, err := getLeafCursor(setupCtx, s.proc)
	if err != nil {
		release()
		return "", err
	}

	sub := s.proc.subscribe()

	cmd := map[string]any{"type": "prompt", "message": message}
	if len(images) > 0 {
		payload := make([]imagePayload, len(images))
		for i, img := range images {
			payload[i] = imagePayload{Type: "image", Data: img.Base64Data, MimeType: img.MimeType}
		}
		cmd["images"] = payload
	}
	if _, err := s.proc.callOK(setupCtx, cmd); err != nil {
		sub.Cancel()
		release()
		return "", err
	}

	t := &turnState{done: make(chan struct{})}
	m.mu.Lock()
	s.turn = t
	m.mu.Unlock()

	go m.finishDetached(s, t, sub)

	m.logger.Info("pi turn submitted",
		slog.String("session_id", s.id),
		slog.String("turn_cursor", cursor),
	)
	return cursor, nil
}

// finishDetached consumes events until the submitted run settles (or the
// process exits), then releases the session. It holds the run slot for the
// whole run, which also keeps the idle janitor away.
func (m *Manager) finishDetached(s *session, t *turnState, sub *subscription) {
	defer func() {
		m.mu.Lock()
		if s.turn == t {
			s.turn = nil
		}
		m.mu.Unlock()
		close(t.done)
		<-s.runSlot
		m.touch(s)
	}()
	defer sub.Cancel()

	// Background context: nothing cancels a detached run except Abort or the
	// process dying (which closes the subscription).
	if _, err := m.waitSettled(context.Background(), s.proc, sub, nil); err != nil {
		m.logger.Warn("submitted pi run ended without settling",
			slog.String("session_id", s.id), slog.Any("error", err))
		return
	}
	m.logger.Info("submitted pi run settled", slog.String("session_id", s.id))
}

// Turn reports the state of the run submitted at cursor. wait=0 answers
// immediately; wait>0 (capped at maxTurnWait) waits for the run to settle
// first. Completion is judged from the session entries after the cursor, not
// process state, so a restart mid-run yields an honest "did not finish"
// (Running=false, Result=nil) rather than a stale previous answer.
func (m *Manager) Turn(ctx context.Context, id, cursor string, wait time.Duration) (TurnStatus, error) {
	if wait > maxTurnWait {
		wait = maxTurnWait
	}
	deadline := time.Now().Add(wait)

	s, err := m.attach(ctx, id)
	if err != nil {
		return TurnStatus{}, err
	}

	for {
		m.mu.Lock()
		t := s.turn
		m.mu.Unlock()

		running := t != nil
		if !running {
			// A run driven by unary Send also occupies the session; report it
			// via pi's own streaming flag.
			state, err := getState(ctx, s.proc)
			if err != nil {
				return TurnStatus{}, err
			}
			running = state.IsStreaming
		}
		if !running {
			break
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			m.touch(s)
			return TurnStatus{Running: true}, nil
		}
		if t != nil {
			select {
			case <-t.done:
			case <-time.After(remaining):
			case <-ctx.Done():
				return TurnStatus{}, ctx.Err()
			}
		} else {
			select {
			case <-time.After(min(remaining, 500*time.Millisecond)):
			case <-ctx.Done():
				return TurnStatus{}, ctx.Err()
			}
		}
	}

	result, err := m.turnResult(ctx, s.proc, cursor)
	if err != nil {
		return TurnStatus{}, err
	}
	m.touch(s)
	return TurnStatus{Running: false, Result: result}, nil
}

// turnResult reads the entries after cursor and extracts the turn's outcome
// from the last assistant message, or nil when the turn produced none.
func (m *Manager) turnResult(ctx context.Context, proc *process, cursor string) (*SendResult, error) {
	cmd := map[string]any{"type": "get_entries"}
	if cursor != "" {
		cmd["since"] = cursor
	}
	data, err := proc.callOK(ctx, cmd)
	if err != nil {
		// pi reports an unknown `since` id as "Entry not found: <id>".
		if strings.Contains(err.Error(), "Entry not found") {
			return nil, fmt.Errorf("%w: %s", ErrBadCursor, cursor)
		}
		return nil, err
	}
	var payload entriesData
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode get_entries: %w", err)
	}

	var last *entryData
	for i := range payload.Entries {
		e := &payload.Entries[i]
		if e.Type == "message" && e.Message.Role == "assistant" {
			last = e
		}
	}
	if last == nil {
		return nil, nil
	}

	text := ""
	for _, block := range last.Message.Content {
		if block.Type == "text" {
			text += block.Text
		}
	}
	stats, err := getStats(ctx, proc)
	if err != nil {
		return nil, err
	}
	return &SendResult{Text: text, StopReason: last.Message.StopReason, Stats: stats}, nil
}

// getLeafCursor returns the session's current leaf entry id, "" when the
// session has no entries yet.
func getLeafCursor(ctx context.Context, proc *process) (string, error) {
	data, err := proc.callOK(ctx, map[string]any{"type": "get_entries"})
	if err != nil {
		return "", err
	}
	var payload entriesData
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("decode get_entries: %w", err)
	}
	if payload.LeafID == nil {
		return "", nil
	}
	return *payload.LeafID, nil
}

// waitSettled consumes events until agent_settled, tracking the last
// assistant stop reason. Client cancellation aborts the in-flight run.
func (m *Manager) waitSettled(ctx context.Context, proc *process, sub *subscription, onEvent func(string, []byte) error) (string, error) {
	stopReason := ""
	abort := func() {
		abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := proc.callOK(abortCtx, map[string]any{"type": "abort"}); err != nil {
			m.logger.Warn("pi abort after cancellation failed", slog.Any("error", err))
		}
	}

	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return "", proc.exitError()
			}
			if ev.Type == "message_end" {
				var end assistantEnd
				if json.Unmarshal(ev.raw, &end) == nil && end.Message.Role == "assistant" {
					stopReason = end.Message.StopReason
				}
			}
			if onEvent != nil {
				if err := onEvent(ev.Type, ev.raw); err != nil {
					abort()
					return "", err
				}
			}
			if ev.Type == "agent_settled" {
				return stopReason, nil
			}
		case <-ctx.Done():
			abort()
			return "", ctx.Err()
		case <-proc.done:
			return "", proc.exitError()
		}
	}
}

// AvailableModels reports the models pi can use. With an id it asks that
// session's process, re-attaching it if needed; with an empty id it answers
// for the box as a whole, without a session.
func (m *Manager) AvailableModels(ctx context.Context, id string) ([]Model, error) {
	if id == "" {
		return m.boxModels(ctx)
	}
	s, err := m.attach(ctx, id)
	if err != nil {
		return nil, err
	}
	models, err := getAvailableModels(ctx, s.proc)
	if err != nil {
		return nil, err
	}
	m.touch(s)
	return models, nil
}

// boxModels answers a session-less catalog query: from cache while it is
// fresh, otherwise from one transient pi process. Concurrent callers are
// collapsed, so a burst of dropdown loads costs a single spawn.
func (m *Manager) boxModels(ctx context.Context) ([]Model, error) {
	if models, ok := m.cachedModels(); ok {
		return models, nil
	}

	select {
	case m.catalogGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-m.catalogGate }()

	// A queued caller inherits whatever the winner just fetched.
	if models, ok := m.cachedModels(); ok {
		return models, nil
	}

	models, err := m.fetchBoxModels(ctx)
	if err != nil {
		// The catalog is near-static, so a stale answer beats no answer when
		// the transient process cannot run (pi misconfigured, box out of
		// resources) — a picker keeps working off the last known catalog.
		if stale := m.staleModels(); stale != nil {
			m.logger.Warn("serving stale pi model catalog", slog.Any("error", err))
			return stale, nil
		}
		return nil, err
	}

	m.mu.Lock()
	m.catalog = models
	m.catalogAt = time.Now()
	m.mu.Unlock()
	return models, nil
}

// fetchBoxModels spawns an ephemeral `pi --mode rpc --no-session`, reads the
// catalog, and tears the process down. It deliberately bypasses the
// MaxSessions accounting: the process holds no session, lives for one call,
// and a box at its session cap must still be able to answer a catalog query.
func (m *Manager) fetchBoxModels(ctx context.Context) ([]Model, error) {
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()
	if stopped {
		return nil, errors.New("manager stopped")
	}

	proc, err := startProcess(m.logger, m.cfg.Bin, []string{"--mode", "rpc", "--no-session"}, "")
	if err != nil {
		return nil, err
	}
	defer proc.stop()

	callCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()
	return getAvailableModels(callCtx, proc)
}

// cachedModels returns the cached catalog while it is within its TTL.
func (m *Manager) cachedModels() ([]Model, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.catalog == nil || time.Since(m.catalogAt) > modelCatalogTTL {
		return nil, false
	}
	return m.catalog, true
}

// staleModels returns the cached catalog regardless of its age, nil when
// nothing was ever cached.
func (m *Manager) staleModels() []Model {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.catalog
}

func getAvailableModels(ctx context.Context, proc *process) ([]Model, error) {
	data, err := proc.callOK(ctx, map[string]any{"type": "get_available_models"})
	if err != nil {
		return nil, err
	}
	var payload struct {
		Models []modelData `json:"models"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode get_available_models: %w", err)
	}

	models := make([]Model, len(payload.Models))
	for i, md := range payload.Models {
		models[i] = Model{
			ID:             md.ID,
			Provider:       md.Provider,
			Name:           md.Name,
			API:            md.API,
			Reasoning:      md.Reasoning,
			Input:          md.Input,
			ContextWindow:  md.ContextWindow,
			MaxTokens:      md.MaxTokens,
			CostInput:      md.Cost.Input,
			CostOutput:     md.Cost.Output,
			CostCacheRead:  md.Cost.CacheRead,
			CostCacheWrite: md.Cost.CacheWrite,
		}
	}
	return models, nil
}

// Abort cancels the in-flight run, if any. Aborting an inactive session is a
// no-op: nothing is running.
func (m *Manager) Abort(ctx context.Context, id string) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	_, err := s.proc.callOK(ctx, map[string]any{"type": "abort"})
	return err
}

// Delete stops the session process; with purge it also removes the session
// file from pi's session directory.
func (m *Manager) Delete(ctx context.Context, id string, purge bool) error {
	file := ""
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
		file = s.file
	}
	m.mu.Unlock()

	if ok {
		s.proc.stop()
	}

	if !purge {
		return nil
	}
	if file == "" {
		// Inactive session: attach briefly to discover the file path.
		attached, err := m.attach(ctx, id)
		if err != nil {
			return err
		}
		file = attached.file
		m.mu.Lock()
		delete(m.sessions, id)
		m.mu.Unlock()
		attached.proc.stop()
	}
	if file == "" {
		return nil // ephemeral session, nothing on disk
	}
	if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove session file: %w", err)
	}
	return nil
}

// ImageInput is one prompt image.
type ImageInput struct {
	MimeType   string
	Base64Data string
}

// attach returns the active session, re-spawning `pi --mode rpc --session id`
// for a session that only exists on disk. Concurrent attaches for the same ID
// are collapsed.
func (m *Manager) attach(ctx context.Context, id string) (*session, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: empty session id", ErrNotFound)
	}
	for {
		m.mu.Lock()
		if m.stopped {
			m.mu.Unlock()
			return nil, errors.New("manager stopped")
		}
		if s, ok := m.sessions[id]; ok {
			m.mu.Unlock()
			return s, nil
		}
		if wait, ok := m.inflight[id]; ok {
			m.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		wait := make(chan struct{})
		m.inflight[id] = wait
		m.mu.Unlock()

		s, err := m.reattach(ctx, id)

		m.mu.Lock()
		delete(m.inflight, id)
		if err == nil {
			m.sessions[id] = s
		}
		m.mu.Unlock()
		close(wait)

		if err != nil {
			return nil, err
		}
		return s, nil
	}
}

func (m *Manager) reattach(ctx context.Context, id string) (*session, error) {
	// pi resolves `--session <id>` against the process working directory: a
	// session recorded under a different cwd is only found by the global
	// search, which prompts interactively and would wedge a headless RPC
	// process. Spawn in the session's own cwd so the lookup stays local; pi
	// then restores that cwd from the session header anyway.
	cwd := m.sessionCwd(id)
	if cwd != "" {
		if fi, err := os.Stat(cwd); err != nil || !fi.IsDir() {
			m.logger.Warn("pi session cwd no longer exists, re-attaching from process cwd",
				slog.String("session_id", id), slog.String("cwd", cwd))
			cwd = ""
		}
	}

	args := m.appendSessionDir([]string{"--mode", "rpc", "--session", id})
	proc, err := m.spawn(args, cwd)
	if err != nil {
		return nil, err
	}

	setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()

	state, err := getState(setupCtx, proc)
	if err != nil {
		proc.stop()
		return nil, fmt.Errorf("%w: %s (%v)", ErrNotFound, id, err)
	}
	if state.SessionID != id {
		// pi silently starts a fresh session for an unknown --session value;
		// a mismatched ID means the requested session does not exist.
		proc.stop()
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}

	if cwd != "" {
		m.mu.Lock()
		m.cwds[id] = cwd
		m.mu.Unlock()
	}

	m.logger.Info("pi session re-attached", slog.String("session_id", id), slog.String("cwd", cwd))
	return &session{
		id:       id,
		file:     state.SessionFile,
		cwd:      cwd,
		proc:     proc,
		runSlot:  make(chan struct{}, 1),
		lastUsed: time.Now(),
	}, nil
}

// sessionCwd recovers the working directory of a session that is not active:
// from the in-memory map first, then from the cwd pi records in the session
// file header (covers sessions created before a server restart).
func (m *Manager) sessionCwd(id string) string {
	m.mu.Lock()
	cwd := m.cwds[id]
	m.mu.Unlock()
	if cwd != "" {
		return cwd
	}
	return findSessionCwd(m.sessionRoot(), id)
}

// sessionRoot is the directory holding pi's session files: the configured
// override, or pi's default ~/.pi/agent/sessions.
func (m *Manager) sessionRoot() string {
	if m.cfg.SessionDir != "" {
		return m.cfg.SessionDir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".pi", "agent", "sessions")
}

func (m *Manager) spawn(args []string, dir string) (*process, error) {
	m.mu.Lock()
	if len(m.sessions)+len(m.inflight) >= m.cfg.MaxSessions {
		m.mu.Unlock()
		return nil, ErrTooManySessions
	}
	m.mu.Unlock()
	return startProcess(m.logger, m.cfg.Bin, args, dir)
}

func (m *Manager) appendSessionDir(args []string) []string {
	if m.cfg.SessionDir != "" {
		args = append(args, "--session-dir", m.cfg.SessionDir)
	}
	return args
}

func (m *Manager) touch(s *session) {
	m.mu.Lock()
	s.lastUsed = time.Now()
	m.mu.Unlock()
}

// janitor stops sessions that have been idle past the timeout. A session
// holding its run slot is mid-prompt and never collected.
func (m *Manager) janitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.janitorStop:
			return
		case <-ticker.C:
		}

		cutoff := time.Now().Add(-m.cfg.IdleTimeout)
		var expired []*session
		m.mu.Lock()
		for id, s := range m.sessions {
			if s.lastUsed.After(cutoff) {
				continue
			}
			select {
			case s.runSlot <- struct{}{}: // idle: take the slot so no run can start
				delete(m.sessions, id)
				expired = append(expired, s)
			default: // mid-prompt
			}
		}
		m.mu.Unlock()

		for _, s := range expired {
			m.logger.Info("stopping idle pi session", slog.String("session_id", s.id))
			s.proc.stop()
		}
	}
}

func getState(ctx context.Context, proc *process) (stateData, error) {
	data, err := proc.callOK(ctx, map[string]any{"type": "get_state"})
	if err != nil {
		return stateData{}, err
	}
	var state stateData
	if err := json.Unmarshal(data, &state); err != nil {
		return stateData{}, fmt.Errorf("decode get_state: %w", err)
	}
	return state, nil
}

func getStats(ctx context.Context, proc *process) (Stats, error) {
	data, err := proc.callOK(ctx, map[string]any{"type": "get_session_stats"})
	if err != nil {
		return Stats{}, err
	}
	var stats statsData
	if err := json.Unmarshal(data, &stats); err != nil {
		return Stats{}, fmt.Errorf("decode get_session_stats: %w", err)
	}
	out := Stats{
		Input:      stats.Tokens.Input,
		Output:     stats.Tokens.Output,
		CacheRead:  stats.Tokens.CacheRead,
		CacheWrite: stats.Tokens.CacheWrite,
		Cost:       stats.Cost,
	}
	if stats.ContextUsage != nil && stats.ContextUsage.Percent != nil {
		out.ContextPercent = int32(*stats.ContextUsage.Percent)
	}
	return out, nil
}

func getLastAssistantText(ctx context.Context, proc *process) (string, error) {
	data, err := proc.callOK(ctx, map[string]any{"type": "get_last_assistant_text"})
	if err != nil {
		return "", err
	}
	var payload struct {
		Text *string `json:"text"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("decode get_last_assistant_text: %w", err)
	}
	if payload.Text == nil {
		return "", nil
	}
	return *payload.Text, nil
}

// infoWithCwd builds an Info from pi's state plus the cwd the manager tracks
// (get_state does not report a working directory).
func infoWithCwd(state stateData, cwd string) Info {
	model := ""
	if state.Model != nil {
		model = state.Model.Provider + "/" + state.Model.ID
	}
	return Info{
		ID:           state.SessionID,
		Name:         state.SessionName,
		File:         state.SessionFile,
		Model:        model,
		Streaming:    state.IsStreaming,
		MessageCount: state.MessageCount,
		Cwd:          cwd,
	}
}
