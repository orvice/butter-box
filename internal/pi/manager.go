package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Sentinel errors the service layer maps to RPC codes.
var (
	ErrBusy            = errors.New("session is processing another message")
	ErrTooManySessions = errors.New("active session limit reached")
	ErrNotFound        = errors.New("session not found")
)

const (
	defaultMaxSessions  = 8
	defaultIdleTimeout  = 30 * time.Minute
	defaultSetupTimeout = 60 * time.Second
	janitorInterval     = time.Minute
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
}

// Info is a session state snapshot.
type Info struct {
	ID           string
	Name         string
	File         string
	Model        string
	Streaming    bool
	MessageCount int32
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

// SendResult is the outcome of one settled prompt run.
type SendResult struct {
	Text       string
	StopReason string
	Stats      Stats
}

type session struct {
	id       string
	file     string
	proc     *process
	runSlot  chan struct{} // capacity 1: holding the token = running a prompt
	lastUsed time.Time
}

// Manager owns the pi session processes on this box.
type Manager struct {
	cfg    Config
	logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	inflight map[string]chan struct{} // attach singleflight, keyed by session ID
	stopped  bool

	janitorStop chan struct{}
}

func NewManager(logger *slog.Logger, cfg Config) *Manager {
	m := &Manager{
		cfg:         cfg.withDefaults(),
		logger:      logger,
		sessions:    map[string]*session{},
		inflight:    map[string]chan struct{}{},
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

	proc, err := m.spawn(args)
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
	m.mu.Unlock()

	m.logger.Info("pi session created",
		slog.String("session_id", s.id),
		slog.String("session_file", s.file),
	)
	return infoFromState(state), nil
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
			infos = append(infos, Info{ID: s.id, File: s.file})
			continue
		}
		infos = append(infos, infoFromState(state))
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
	return infoFromState(state), stats, nil
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
	args := m.appendSessionDir([]string{"--mode", "rpc", "--session", id})
	proc, err := m.spawn(args)
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

	m.logger.Info("pi session re-attached", slog.String("session_id", id))
	return &session{
		id:       id,
		file:     state.SessionFile,
		proc:     proc,
		runSlot:  make(chan struct{}, 1),
		lastUsed: time.Now(),
	}, nil
}

func (m *Manager) spawn(args []string) (*process, error) {
	m.mu.Lock()
	if len(m.sessions)+len(m.inflight) >= m.cfg.MaxSessions {
		m.mu.Unlock()
		return nil, ErrTooManySessions
	}
	m.mu.Unlock()
	return startProcess(m.logger, m.cfg.Bin, args)
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

func infoFromState(state stateData) Info {
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
	}
}
