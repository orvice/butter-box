package cursor

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/orvice/butter-box/internal/sandbox"
	sdkv1 "github.com/orvice/butter-box/pkg/proto/sdk/v1"
)

const (
	defaultMaxSessions  = 8
	defaultIdleTimeout  = 30 * time.Minute
	defaultSetupTimeout = 60 * time.Second
	janitorInterval     = time.Minute
	bridgeCancelTimeout = 5 * time.Second
)

// Config configures Cursor SDK Bridge sessions on this box.
type Config struct {
	// Bin is the cursor-sdk-bridge executable. Defaults to
	// cursor-sdk-bridge.
	Bin string
	// APIKey is passed to the bridge environment and SDK requests. It is
	// never returned by the service or written to logs.
	APIKey string
	// MaxSessions caps concurrently active bridge processes.
	MaxSessions int
	// IdleTimeout stops a bridge process after inactivity. Durable Cursor
	// agent state remains on disk and is resumed on the next use.
	IdleTimeout time.Duration
	// SandboxRoot bounds caller-supplied working directories.
	SandboxRoot string
}

func (c Config) withDefaults() Config {
	if c.Bin == "" {
		c.Bin = "cursor-sdk-bridge"
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = defaultMaxSessions
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.SandboxRoot == "" {
		c.SandboxRoot, _ = os.Getwd()
	} else if root, err := filepath.Abs(c.SandboxRoot); err == nil {
		c.SandboxRoot = root
	}
	c.SandboxRoot = filepath.Clean(c.SandboxRoot)
	return c
}

// CreateOpts contains the caller-selectable Cursor agent settings.
type CreateOpts struct {
	Name  string
	Model string
	Mode  string
	Cwd   string
}

// ImageInput is one inline image sent to a Cursor agent.
type ImageInput struct {
	MimeType string
	Data     []byte
}

// Model is one entry in the Cursor account model catalog.
type Model struct {
	ID   string
	Name string
}

type session struct {
	id       string
	cwd      string
	bridge   *bridge
	runSlot  chan struct{}
	runID    string
	lastUsed time.Time
}

// Manager owns Cursor SDK Bridge processes and their durable agent sessions.
type Manager struct {
	cfg    Config
	logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	cwds     map[string]string
	inflight map[string]chan struct{}
	starting int
	stopped  bool

	janitorStop chan struct{}
	stopOnce    sync.Once
}

func NewManager(logger *slog.Logger, cfg Config) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		cfg:         cfg.withDefaults(),
		logger:      logger,
		sessions:    map[string]*session{},
		cwds:        map[string]string{},
		inflight:    map[string]chan struct{}{},
		janitorStop: make(chan struct{}),
	}
	go m.janitor()
	return m
}

// Stop terminates all active bridge processes. Durable agent state is kept.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.janitorStop)
		m.mu.Lock()
		m.stopped = true
		bridges := make([]*bridge, 0, len(m.sessions))
		for id, s := range m.sessions {
			bridges = append(bridges, s.bridge)
			delete(m.sessions, id)
		}
		m.mu.Unlock()
		for _, b := range bridges {
			b.stop()
		}
	})
}

// Create starts a bridge and creates one local Cursor agent.
func (m *Manager) Create(ctx context.Context, opts CreateOpts) (string, error) {
	ctx = nonNilContext(ctx)
	cwd, err := m.resolveCwd(opts.Cwd)
	if err != nil {
		return "", err
	}
	mode, err := parseMode(opts.Mode)
	if err != nil {
		return "", err
	}
	if err := m.beginStart(); err != nil {
		return "", err
	}

	setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()
	b, err := startBridge(setupCtx, m.logger, m.cfg.Bin, m.cfg.SandboxRoot, m.cfg.APIKey)
	if err != nil {
		m.releaseStart()
		return "", err
	}

	optsMessage := &sdkv1.AgentOptions{
		ApiKey: m.cfg.APIKey,
		Name:   opts.Name,
		Mode:   mode,
		Local:  &sdkv1.LocalAgentOptions{Cwd: []string{cwd}},
	}
	if opts.Model != "" {
		optsMessage.Model = &sdkv1.ModelSelection{Id: opts.Model}
	}
	response, err := b.agent.CreateAgent(setupCtx, connect.NewRequest(&sdkv1.CreateAgentRequest{
		Options: optsMessage,
	}))
	if err != nil {
		b.stop()
		m.releaseStart()
		return "", normalizeRPCError(err)
	}
	id := response.Msg.GetAgentId()
	if id == "" {
		b.stop()
		m.releaseStart()
		return "", errors.New("cursor bridge returned an empty agent ID")
	}

	s := &session{
		id:       id,
		cwd:      cwd,
		bridge:   b,
		runSlot:  make(chan struct{}, 1),
		lastUsed: time.Now(),
	}
	if err := m.commitSession(s); err != nil {
		b.stop()
		return "", err
	}
	m.logger.Info("cursor session created", slog.String("session_id", id), slog.String("cwd", cwd))
	return id, nil
}

// Send sends one message and returns only after a terminal successful run.
func (m *Manager) Send(ctx context.Context, id, message string, images []ImageInput) (string, error) {
	ctx = nonNilContext(ctx)
	s, err := m.attach(ctx, id)
	if err != nil {
		return "", err
	}
	select {
	case s.runSlot <- struct{}{}:
	default:
		return "", ErrBusy
	}
	defer func() {
		<-s.runSlot
		m.touch(s)
	}()

	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			_ = m.abortAndStop(s)
		case <-stopWatch:
		}
	}()
	defer func() {
		close(stopWatch)
		<-watchDone
		m.clearRun(s)
	}()

	stream, err := s.bridge.agent.Send(ctx, connect.NewRequest(&sdkv1.SendRequest{
		AgentId: id,
		Message: &sdkv1.UserMessage{
			Text:   message,
			Images: sdkImages(images),
		},
	}))
	if err != nil {
		if ctx.Err() != nil {
			_ = m.abortAndStop(s)
			return "", ctx.Err()
		}
		return "", normalizeRPCError(err)
	}

	text, err := m.consumeRun(s, stream)
	if err != nil {
		if ctx.Err() != nil {
			_ = m.abortAndStop(s)
			return "", ctx.Err()
		}
		return "", normalizeRPCError(err)
	}
	return text, nil
}

func (m *Manager) consumeRun(s *session, stream *connect.ServerStreamForClient[sdkv1.RunStreamMessage]) (string, error) {
	defer func() { _ = stream.Close() }()

	var terminal *sdkv1.RunStreamResult
	var assistantText string
	statusMessage := ""
	for stream.Receive() {
		message := stream.Msg()
		if runID := streamRunID(message); runID != "" {
			m.setRunID(s, runID)
		}
		switch {
		case message.GetSdkMessage() != nil:
			sdkMessage := message.GetSdkMessage()
			if sdkMessage.GetType() == "assistant" {
				assistantText += assistantMessageText(sdkMessage)
			}
			if sdkMessage.GetType() == "status" {
				if current := statusMessageText(sdkMessage.GetMessage()); current != "" {
					statusMessage = current
				}
			}
		case message.GetResult() != nil:
			terminal = message.GetResult()
		}
	}
	if err := stream.Err(); err != nil {
		select {
		case <-s.bridge.done:
			return "", s.bridge.exitError()
		default:
			return "", err
		}
	}
	if terminal == nil {
		return "", ErrRunIncomplete
	}

	status := runStatusName(terminal.GetStatus())
	if status != "finished" {
		errorCode := terminal.GetErrorCode()
		return "", runFailure(status, errorCode, statusMessage)
	}
	if result := terminal.GetResult(); result != nil && result.GetResult() != "" {
		return result.GetResult(), nil
	}
	return assistantText, nil
}

// ListModels starts a transient bridge and never consumes a persistent session
// slot.
func (m *Manager) ListModels(ctx context.Context) ([]Model, error) {
	ctx = nonNilContext(ctx)
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()
	if stopped {
		return nil, errors.New("cursor manager stopped")
	}

	setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
	defer cancel()
	b, err := startBridge(setupCtx, m.logger, m.cfg.Bin, m.cfg.SandboxRoot, m.cfg.APIKey)
	if err != nil {
		return nil, err
	}
	defer b.stop()

	response, err := b.cursor.ListModels(setupCtx, connect.NewRequest(&sdkv1.ListModelsRequest{
		Options: &sdkv1.CursorRequestOptions{ApiKey: m.cfg.APIKey},
	}))
	if err != nil {
		return nil, normalizeRPCError(err)
	}
	models := make([]Model, 0, len(response.Msg.GetItems()))
	for _, model := range response.Msg.GetItems() {
		models = append(models, Model{ID: model.GetId(), Name: model.GetDisplayName()})
	}
	return models, nil
}

// Abort cancels a run when one is known and always stops the session's bridge.
// The durable agent state is intentionally left untouched.
func (m *Manager) Abort(_ context.Context, id string) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	_, known := m.cwds[id]
	m.mu.Unlock()
	if !ok {
		if known {
			return nil
		}
		return ErrNotFound
	}
	return m.abortAndStop(s)
}

func (m *Manager) abortAndStop(s *session) error {
	runID := m.currentRunID(s)
	var cancelErr error
	if runID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), bridgeCancelTimeout)
		cancelErr = s.bridge.cancelRun(ctx, runID)
		cancel()
	}
	m.removeSession(s)
	s.bridge.stop()
	if cancelErr == nil {
		return nil
	}
	normalized := normalizeRPCError(cancelErr)
	if errors.Is(normalized, errBridgeExited) || errors.Is(normalized, ErrNotFound) {
		return nil
	}
	return normalized
}

// attach reuses an active session or resumes a durable agent in a fresh
// bridge. Concurrent attaches for the same ID share one startup.
func (m *Manager) attach(ctx context.Context, id string) (*session, error) {
	ctx = nonNilContext(ctx)
	if strings.TrimSpace(id) == "" {
		return nil, ErrNotFound
	}
	for {
		m.mu.Lock()
		if m.stopped {
			m.mu.Unlock()
			return nil, errors.New("cursor manager stopped")
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
		if len(m.sessions)+m.starting >= m.cfg.MaxSessions {
			m.mu.Unlock()
			return nil, ErrTooManySessions
		}
		wait := make(chan struct{})
		m.inflight[id] = wait
		m.starting++
		m.mu.Unlock()

		setupCtx, cancel := context.WithTimeout(ctx, defaultSetupTimeout)
		b, err := startBridge(setupCtx, m.logger, m.cfg.Bin, m.cfg.SandboxRoot, m.cfg.APIKey)
		if err == nil {
			_, err = b.agent.ResumeAgent(setupCtx, connect.NewRequest(&sdkv1.ResumeAgentRequest{
				AgentId: id,
				Options: &sdkv1.AgentOptions{ApiKey: m.cfg.APIKey},
			}))
			if err != nil {
				err = normalizeRPCError(err)
			}
		}

		cwd := m.sessionCwd(id)
		if err == nil {
			// GetAgent is not needed for the normal in-memory path, but it
			// recovers the original cwd after a server restart. Older test
			// doubles may omit this optional call, so only Unimplemented is
			// ignored.
			info, infoErr := b.agent.GetAgent(setupCtx, connect.NewRequest(&sdkv1.GetAgentRequest{
				AgentId: id,
				Options: &sdkv1.AgentOperationOptions{ApiKey: m.cfg.APIKey},
			}))
			if infoErr == nil {
				if local := info.Msg.GetAgent().GetLocal(); local != nil && local.GetCwd() != "" {
					cwd = local.GetCwd()
				}
			} else if !isConnectCode(infoErr, connect.CodeUnimplemented) {
				err = normalizeRPCError(infoErr)
			}
		}
		cancel()

		if err != nil {
			if b != nil {
				b.stop()
			}
		}

		var result *session
		m.mu.Lock()
		m.starting--
		delete(m.inflight, id)
		if err == nil && !m.stopped {
			if cwd == "" {
				cwd = m.cfg.SandboxRoot
			}
			result = &session{
				id:       id,
				cwd:      cwd,
				bridge:   b,
				runSlot:  make(chan struct{}, 1),
				lastUsed: time.Now(),
			}
			m.sessions[id] = result
			m.cwds[id] = cwd
		}
		stopped := m.stopped
		m.mu.Unlock()
		close(wait)

		if stopped && result != nil {
			result.bridge.stop()
			return nil, errors.New("cursor manager stopped")
		}
		if stopped && b != nil {
			b.stop()
			return nil, errors.New("cursor manager stopped")
		}
		if err != nil {
			return nil, err
		}
		go m.watchSession(result)
		return result, nil
	}
}

func (m *Manager) beginStart() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return errors.New("cursor manager stopped")
	}
	if len(m.sessions)+m.starting >= m.cfg.MaxSessions {
		return ErrTooManySessions
	}
	m.starting++
	return nil
}

func (m *Manager) releaseStart() {
	m.mu.Lock()
	if m.starting > 0 {
		m.starting--
	}
	m.mu.Unlock()
}

func (m *Manager) commitSession(s *session) error {
	m.mu.Lock()
	m.starting--
	if m.stopped {
		m.mu.Unlock()
		return errors.New("cursor manager stopped")
	}
	if _, exists := m.sessions[s.id]; exists {
		m.mu.Unlock()
		return fmt.Errorf("cursor session %s already exists", s.id)
	}
	m.sessions[s.id] = s
	m.cwds[s.id] = s.cwd
	m.mu.Unlock()
	go m.watchSession(s)
	return nil
}

func (m *Manager) watchSession(s *session) {
	<-s.bridge.done
	m.mu.Lock()
	if m.sessions[s.id] == s {
		delete(m.sessions, s.id)
	}
	m.mu.Unlock()
}

func (m *Manager) sessionCwd(id string) string {
	m.mu.Lock()
	cwd := m.cwds[id]
	m.mu.Unlock()
	return cwd
}

func (m *Manager) setRunID(s *session, runID string) {
	m.mu.Lock()
	if m.sessions[s.id] == s {
		s.runID = runID
	}
	m.mu.Unlock()
}

func (m *Manager) currentRunID(s *session) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return s.runID
}

func (m *Manager) clearRun(s *session) {
	m.mu.Lock()
	if s.runID != "" {
		s.runID = ""
	}
	m.mu.Unlock()
}

func (m *Manager) removeSession(s *session) {
	m.mu.Lock()
	if m.sessions[s.id] == s {
		delete(m.sessions, s.id)
	}
	m.mu.Unlock()
}

func (m *Manager) touch(s *session) {
	m.mu.Lock()
	s.lastUsed = time.Now()
	m.mu.Unlock()
}

func (m *Manager) janitor() {
	interval := janitorInterval
	if half := m.cfg.IdleTimeout / 2; half > 0 && half < interval {
		interval = half
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
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
			case s.runSlot <- struct{}{}:
				delete(m.sessions, id)
				expired = append(expired, s)
			default:
			}
		}
		m.mu.Unlock()

		for _, s := range expired {
			m.logger.Info("stopping idle cursor session", slog.String("session_id", s.id))
			s.bridge.stop()
		}
	}
}

func (m *Manager) resolveCwd(userPath string) (string, error) {
	if m.cfg.SandboxRoot == "" {
		return "", fmt.Errorf("%w: no sandbox root configured", ErrInvalidCwd)
	}
	resolved := m.cfg.SandboxRoot
	if strings.TrimSpace(userPath) != "" {
		var err error
		resolved, err = sandbox.Resolve(m.cfg.SandboxRoot, userPath)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidCwd, err)
		}
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%w: %q is not an existing directory", ErrInvalidCwd, resolved)
	}
	return resolved, nil
}

func parseMode(mode string) (sdkv1.AgentModeOption, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "agent":
		return sdkv1.AgentModeOption_AGENT_MODE_OPTION_AGENT, nil
	case "plan":
		return sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN, nil
	default:
		return sdkv1.AgentModeOption_AGENT_MODE_OPTION_UNSPECIFIED, fmt.Errorf("%w: %q", ErrInvalidMode, mode)
	}
}

func sdkImages(images []ImageInput) []*sdkv1.SdkImage {
	if len(images) == 0 {
		return nil
	}
	out := make([]*sdkv1.SdkImage, len(images))
	for i, image := range images {
		out[i] = &sdkv1.SdkImage{
			Source: &sdkv1.SdkImage_Data{Data: &sdkv1.SdkImageData{
				Data:     base64.StdEncoding.EncodeToString(image.Data),
				MimeType: image.MimeType,
			}},
		}
	}
	return out
}

func streamRunID(message *sdkv1.RunStreamMessage) string {
	if result := message.GetResult(); result != nil && result.GetRunId() != "" {
		return result.GetRunId()
	}
	if done := message.GetDone(); done != nil && done.GetRunId() != "" {
		return done.GetRunId()
	}
	if sdkMessage := message.GetSdkMessage(); sdkMessage != nil {
		return structString(sdkMessage.GetMessage(), "run_id", "runId")
	}
	return ""
}

func assistantMessageText(message *sdkv1.SdkMessage) string {
	payload := message.GetMessage()
	if payload == nil {
		return ""
	}
	if nested := structValue(payload, "message"); nested != nil {
		payload = nested
	}
	if text := structString(payload, "text"); text != "" {
		return text
	}
	content := structList(payload, "content")
	var result strings.Builder
	for _, value := range content {
		block := value.GetStructValue()
		if block == nil || structString(block, "type") != "text" {
			continue
		}
		result.WriteString(structString(block, "text"))
	}
	return result.String()
}

func statusMessageText(payload *structpb.Struct) string {
	for _, key := range []string{"message", "error", "reason"} {
		if value := structString(payload, key); value != "" {
			return value
		}
	}
	return ""
}

func structString(payload *structpb.Struct, keys ...string) string {
	if payload == nil {
		return ""
	}
	for _, key := range keys {
		if value, ok := payload.GetFields()[key]; ok {
			if text := value.GetStringValue(); text != "" {
				return text
			}
		}
	}
	return ""
}

func structValue(payload *structpb.Struct, key string) *structpb.Struct {
	if payload == nil {
		return nil
	}
	return payload.GetFields()[key].GetStructValue()
}

func structList(payload *structpb.Struct, key string) []*structpb.Value {
	if payload == nil {
		return nil
	}
	return payload.GetFields()[key].GetListValue().GetValues()
}

func runStatusName(status sdkv1.RunLifecycleStatus) string {
	name := status.String()
	name = strings.TrimPrefix(name, "RUN_LIFECYCLE_STATUS_")
	return strings.ToLower(name)
}

func isConnectCode(err error, code connect.Code) bool {
	var connectErr *connect.Error
	return errors.As(err, &connectErr) && connectErr.Code() == code
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
