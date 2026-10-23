package pi

import (
	"bufio"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"uuid"
)

const (
	hostProtocolVersion = 1
	hostMaxPayload      = 256 * 1024
	hostMaxCalls        = 64
	hostMaxRunSeconds   = 86400
	hostMaxError        = 8 * 1024
	hostRetainedTurns   = 8
	hostReceiptTTL      = 10 * time.Minute
)

var (
	ErrHostUnsupported = errors.New("compatible Pi host-tools extension is not ready")
	ErrHostConfig      = errors.New("invalid host-tools configuration or payload")
	ErrHostGone        = errors.New("host-tools turn or call is no longer active")
	ErrHostUnknown     = errors.New("unknown host-tools turn or call")
	ErrHostConflict    = errors.New("conflicting host-call receipt")
)

//go:embed hostextension/extension.ts hostextension/render_ui.json
var hostAssets embed.FS

var hostContract, _ = hostAssets.ReadFile("hostextension/render_ui.json")

func hostContractHash() string { return fmt.Sprintf("%x", sha256.Sum256(hostContract)) }

// HostTools is an explicit, bounded, per-submission authorization.
type HostTools struct {
	ProtocolVersion uint32
	ContractSHA256  string
	AllowedTools    []string
	MaxRunSeconds   uint32
}

func (h *HostTools) validate() error {
	if h.ProtocolVersion != hostProtocolVersion || h.ContractSHA256 != hostContractHash() || len(h.AllowedTools) != 1 || h.AllowedTools[0] != "render_ui" || h.MaxRunSeconds > hostMaxRunSeconds {
		return ErrHostConfig
	}
	return nil
}

type HostCall struct {
	SessionID string
	TurnID    string
	CallID    string
	Tool      string
	Arguments string
}

type hostReceipt struct {
	call        HostCall
	answered    bool
	cancelled   bool
	fingerprint [32]byte
}

type hostGrant struct {
	id      string
	active  bool
	begun   bool
	expires time.Time
	ended   time.Time
	changed chan struct{}
	calls   map[string]*hostReceipt
	timer   *time.Timer
}

func (g *hostGrant) notify() { close(g.changed); g.changed = make(chan struct{}) }

// hostRelay owns a single process's private transport and bounded receipt
// ledger. It never trusts session/turn identities supplied in tool arguments.
type hostRelay struct {
	mu          sync.Mutex
	dir         string
	listener    net.Listener
	conn        net.Conn
	ready       chan struct{}
	sessionID   string
	closed      bool
	terminating atomic.Bool
	closedCh    chan struct{}
	grants      []*hostGrant
	current     *hostGrant
	// Called outside mu when an active grant expires or loses its transport.
	stopProcess func()
}

type hostWire struct {
	Type       string         `json:"type"`
	Version    uint32         `json:"version,omitzero"`
	Hash       string         `json:"hash,omitempty"`
	SessionID  string         `json:"session_id,omitempty"`
	ID         string         `json:"id,omitempty"`
	TurnID     string         `json:"turn_id,omitempty"`
	CallID     string         `json:"call_id,omitempty"`
	Tool       string         `json:"tool,omitempty"`
	Arguments  jsontext.Value `json:"arguments,omitempty"`
	Tools      []string       `json:"tools,omitempty"`
	DeadlineMS int64          `json:"deadline_ms,omitzero"`
	Result     jsontext.Value `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
}

func newHostRelay() (*hostRelay, error) {
	// Keep socket paths short even when TMPDIR is a long test directory.
	dir, err := os.MkdirTemp("/tmp", "bb-pi-")
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	for _, name := range []string{"extension.ts", "render_ui.json"} {
		data, err := hostAssets.ReadFile("hostextension/" + name)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return nil, err
		}
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "relay.sock"))
	if err != nil {
		return nil, err
	}
	cleanup = false
	return &hostRelay{dir: dir, listener: listener, ready: make(chan struct{}), closedCh: make(chan struct{})}, nil
}

func (r *hostRelay) serve() {
	conn, err := r.listener.Accept()
	if err != nil {
		return
	}
	_ = r.listener.Close() // exactly one trusted extension connection
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = conn.Close()
		return
	}
	r.conn = conn
	r.mu.Unlock()
	defer r.disconnect()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 2*hostMaxPayload+hostMaxError)
	first := true
	for scanner.Scan() {
		var msg hostWire
		if json.Unmarshal(scanner.Bytes(), &msg) != nil {
			return
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		if first {
			if msg.Type != "hello" || msg.Version != hostProtocolVersion || msg.Hash != hostContractHash() || msg.SessionID == "" {
				r.mu.Unlock()
				return
			}
			r.sessionID = msg.SessionID
			close(r.ready)
			first = false
		} else {
			r.receiveLocked(msg)
		}
		r.mu.Unlock()
	}
}

func (r *hostRelay) sendLocked(msg hostWire) error {
	if r.conn == nil || r.closed {
		return ErrHostUnsupported
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_ = r.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = r.conn.Write(append(data, '\n'))
	if err != nil {
		_ = r.conn.Close()
	}
	return err
}

func (r *hostRelay) receiveLocked(msg hostWire) {
	g := r.current
	switch msg.Type {
	case "begin":
		reply := hostWire{Type: "config", ID: msg.ID}
		if g != nil && g.active && !g.begun && time.Now().Before(g.expires) {
			g.begun = true
			reply.TurnID, reply.Tools, reply.DeadlineMS = g.id, []string{"render_ui"}, g.expires.UnixMilli()
		}
		_ = r.sendLocked(reply)
	case "call":
		fail := func(reason string) {
			_ = r.sendLocked(hostWire{Type: "result", TurnID: msg.TurnID, CallID: msg.CallID, Error: reason})
		}
		if g == nil || !g.active || !g.begun || g.id != msg.TurnID || !time.Now().Before(g.expires) {
			fail("host tool is not authorized for this turn")
			return
		}
		if msg.Tool != "render_ui" || msg.CallID == "" || len(msg.CallID) > 256 || len(msg.Arguments) > hostMaxPayload || msg.Arguments.Kind() != '{' {
			fail("invalid host tool request")
			return
		}
		if _, exists := g.calls[msg.CallID]; exists {
			fail("duplicate tool call ID")
			return
		}
		if len(g.calls) >= hostMaxCalls {
			fail("host tool call limit reached")
			return
		}
		for _, call := range g.calls {
			if !call.answered && !call.cancelled {
				fail("host tool queue is full; calls must execute serially")
				return
			}
		}
		g.calls[msg.CallID] = &hostReceipt{call: HostCall{SessionID: r.sessionID, TurnID: g.id, CallID: msg.CallID, Tool: msg.Tool, Arguments: string(msg.Arguments)}}
		g.notify()
	case "cancel_call":
		if g != nil && g.id == msg.TurnID {
			if call := g.calls[msg.CallID]; call != nil && !call.answered {
				call.cancelled = true
				call.call.Arguments = ""
				g.notify()
			}
		}
	}
}

func (r *hostRelay) isReady(ctx context.Context, sessionID string) bool {
	select {
	case <-r.ready:
	case <-r.closedCh:
		return false
	case <-ctx.Done():
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.closed && r.sessionID == sessionID
}

func (r *hostRelay) authorize(h *HostTools) (string, error) {
	if err := h.validate(); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.sessionID == "" {
		return "", ErrHostUnsupported
	}
	r.collectLocked()
	// Reserve room for the next grant. The run slot permits only one active
	// grant, so only completed grants can be evicted here.
	for len(r.grants) >= hostRetainedTurns && !r.grants[0].active {
		r.grants = r.grants[1:]
	}
	seconds := h.MaxRunSeconds
	if seconds == 0 {
		seconds = 1800
	}
	g := &hostGrant{id: uuid.New().String(), active: true, expires: time.Now().Add(time.Duration(seconds) * time.Second), changed: make(chan struct{}), calls: map[string]*hostReceipt{}}
	g.timer = time.AfterFunc(time.Until(g.expires), func() { r.expire(g) })
	r.current = g
	r.grants = append(r.grants, g)
	return g.id, nil
}

func (r *hostRelay) expire(g *hostGrant) {
	r.mu.Lock()
	if !g.active {
		r.mu.Unlock()
		return
	}
	r.terminating.Store(true)
	r.revokeLocked(g)
	stop := r.stopProcess
	r.mu.Unlock()
	// Stop this exact process, never look up a later process by session ID.
	if stop != nil {
		stop()
	}
}

func (r *hostRelay) revokeLocked(g *hostGrant) {
	if !g.active {
		return
	}
	g.active = false
	g.ended = time.Now()
	if g.timer != nil {
		g.timer.Stop()
	}
	for _, call := range g.calls {
		if !call.answered && !call.cancelled {
			call.cancelled = true
			call.call.Arguments = ""
			_ = r.sendLocked(hostWire{Type: "result", TurnID: g.id, CallID: call.call.CallID, Error: "host tool authorization ended"})
		}
	}
	g.notify()
}

func (r *hostRelay) revoke() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		r.revokeLocked(r.current)
	}
}

func (r *hostRelay) collectLocked() {
	kept := r.grants[:0]
	for _, g := range r.grants {
		if g.active || time.Since(g.ended) < hostReceiptTTL {
			kept = append(kept, g)
		}
	}
	r.grants = kept
}

func (r *hostRelay) findLocked(turnID string) *hostGrant {
	r.collectLocked()
	for _, g := range r.grants {
		if g.id == turnID && (g.active || time.Since(g.ended) < hostReceiptTTL) {
			return g
		}
	}
	return nil
}

func (r *hostRelay) list(ctx context.Context, turnID string, wait time.Duration) ([]HostCall, bool, error) {
	deadline := time.NewTimer(max(0, min(wait, maxTurnWait)))
	defer deadline.Stop()
	for {
		r.mu.Lock()
		g := r.findLocked(turnID)
		if g == nil || r.closed {
			r.mu.Unlock()
			return nil, false, ErrHostUnknown
		}
		var calls []HostCall
		for _, record := range g.calls {
			if !record.answered && !record.cancelled {
				calls = append(calls, record.call)
			}
		}
		running, changed := g.active, g.changed
		r.mu.Unlock()
		if len(calls) > 0 || !running || wait <= 0 {
			return calls, running, nil
		}
		select {
		case <-changed:
		case <-deadline.C:
			wait = 0
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

func (r *hostRelay) respond(turnID, callID, result, toolError string) error {
	if len(result) > hostMaxPayload || len(toolError) > hostMaxError || (result == "") == (toolError == "") {
		return ErrHostConfig
	}
	var value jsontext.Value
	if result != "" {
		value = jsontext.Value(result)
		if value.Kind() != '{' || value.Canonicalize(jsontext.CanonicalizeRawInts(false), jsontext.CanonicalizeRawFloats(false)) != nil {
			return ErrHostConfig
		}
	}
	// Keep only the digest once delivered; retained turns cannot accumulate
	// 64 large arguments/results each. Preserve numeric precision in receipts.
	fingerprint := sha256.Sum256(append(append([]byte(toolError), 0), value...))
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.findLocked(turnID)
	if g == nil || r.closed {
		return ErrHostUnknown
	}
	record := g.calls[callID]
	if record == nil {
		return ErrHostUnknown
	}
	if record.cancelled {
		return ErrHostGone
	}
	if record.answered {
		if record.fingerprint != fingerprint {
			return ErrHostConflict
		}
		return nil
	}
	if !g.active || !time.Now().Before(g.expires) {
		return ErrHostGone
	}
	if err := r.sendLocked(hostWire{Type: "result", TurnID: turnID, CallID: callID, Result: value, Error: toolError}); err != nil {
		return err
	}
	record.answered, record.fingerprint = true, fingerprint
	record.call.Arguments = ""
	g.notify()
	return nil
}

func (r *hostRelay) disconnect() {
	r.mu.Lock()
	active := r.current != nil && r.current.active
	if active {
		r.terminating.Store(true)
	}
	stop := r.stopProcess
	r.closeLocked()
	r.mu.Unlock()
	if active && stop != nil {
		go stop()
	}
}

func (r *hostRelay) closeLocked() {
	if r.closed {
		return
	}
	if r.current != nil {
		r.revokeLocked(r.current)
	}
	r.closed = true
	close(r.closedCh)
	_ = r.listener.Close()
	if r.conn != nil {
		_ = r.conn.Close()
	}
	_ = os.RemoveAll(r.dir)
}

func (r *hostRelay) stopping() bool {
	if r == nil {
		return false
	}
	return r.terminating.Load()
}

func (r *hostRelay) close() { r.mu.Lock(); defer r.mu.Unlock(); r.closeLocked() }

// startSessionProcess adds only trusted Box-managed resources. The transient
// model-catalog process deliberately does not load this session extension.
func (m *Manager) startSessionProcess(args []string, dir string) (*process, error) {
	relay, err := newHostRelay()
	if err != nil {
		return nil, fmt.Errorf("initialize Pi host relay: %w", err)
	}
	args = append(args, "--extension", filepath.Join(relay.dir, "extension.ts"))
	env := []string{"BUTTERBOX_PI_HOST_SOCKET=" + filepath.Join(relay.dir, "relay.sock"), "BUTTERBOX_PI_HOST_CONTRACT=" + filepath.Join(relay.dir, "render_ui.json")}
	proc, err := startProcessEnv(m.logger, m.cfg.Bin, args, dir, env)
	if err != nil {
		relay.close()
		return nil, err
	}
	proc.host = relay
	relay.stopProcess = proc.stop
	go relay.serve()
	go func() { <-proc.done; relay.close() }()
	return proc, nil
}

func (m *Manager) HostToolsReady(ctx context.Context, id string) (bool, error) {
	s, err := m.attach(ctx, id)
	if err != nil {
		return false, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.proc.host != nil && s.proc.host.isReady(readyCtx, id), nil
}

func (m *Manager) hostProcess(id string) (*hostRelay, error) {
	// Do not attach to fulfill old receipts: restart never restores grants.
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[id]
	if s == nil || s.proc.exited() || s.proc.host == nil {
		return nil, ErrHostUnknown
	}
	return s.proc.host, nil
}

func (m *Manager) ListHostCalls(ctx context.Context, id, turnID string, wait time.Duration) ([]HostCall, bool, error) {
	r, err := m.hostProcess(id)
	if err != nil {
		return nil, false, err
	}
	return r.list(ctx, turnID, wait)
}

func (m *Manager) RespondHostCall(id, turnID, callID, result, toolError string) error {
	r, err := m.hostProcess(id)
	if err != nil {
		return err
	}
	return r.respond(turnID, callID, result, toolError)
}
