package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const processStopGrace = 10 * time.Second

var errProcessExited = errors.New("pi process exited")

// process is one running `pi --mode rpc` child. It owns stdin/stdout framing,
// request/response correlation, event fan-out, and the headless auto-answer
// for extension UI dialogs.
type process struct {
	logger *slog.Logger
	cmd    *exec.Cmd
	stdin  io.WriteCloser

	writeMu sync.Mutex

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan response
	subs    map[int64]*subscription
	nextSub int64

	done    chan struct{}
	exitErr error
}

// startProcess spawns the pi child. dir, when non-empty, becomes the child's
// working directory.
func startProcess(logger *slog.Logger, bin string, args []string, dir string) (*process, error) {
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}

	p := &process{
		logger:  logger,
		cmd:     cmd,
		stdin:   stdin,
		pending: map[string]chan response{},
		subs:    map[int64]*subscription{},
		done:    make(chan struct{}),
	}

	go p.readLoop(stdout)
	return p, nil
}

func (p *process) readLoop(stdout io.Reader) {
	scanErr := scanLines(stdout, func(line []byte) error {
		ev, err := decodeRaw(line)
		if err != nil {
			p.logger.Warn("skipping undecodable pi record", slog.Any("error", err))
			return nil
		}
		p.dispatch(ev)
		return nil
	})

	waitErr := p.cmd.Wait()

	p.mu.Lock()
	p.exitErr = errProcessExited
	if waitErr != nil {
		p.exitErr = fmt.Errorf("%w: %v", errProcessExited, waitErr)
	} else if scanErr != nil {
		p.exitErr = fmt.Errorf("%w: read stdout: %v", errProcessExited, scanErr)
	}
	for id, ch := range p.pending {
		delete(p.pending, id)
		close(ch)
	}
	for id, sub := range p.subs {
		delete(p.subs, id)
		sub.close()
	}
	p.mu.Unlock()
	close(p.done)
}

func (p *process) dispatch(ev rawEvent) {
	switch ev.Type {
	case "response":
		p.mu.Lock()
		ch, ok := p.pending[ev.ID]
		if ok {
			delete(p.pending, ev.ID)
		}
		p.mu.Unlock()
		if !ok {
			p.logger.Warn("pi response without waiter", slog.String("id", ev.ID))
			return
		}
		var resp response
		if err := json.Unmarshal(ev.raw, &resp); err != nil {
			p.logger.Warn("undecodable pi response", slog.Any("error", err))
			close(ch)
			return
		}
		ch <- resp

	case "extension_ui_request":
		if dialogMethods[ev.Method] {
			// Headless: never let an extension dialog block the run.
			reply := map[string]any{"type": "extension_ui_response", "id": ev.ID, "cancelled": true}
			if err := p.write(reply); err != nil {
				p.logger.Warn("failed answering extension UI dialog", slog.Any("error", err))
			}
			p.logger.Info("auto-cancelled pi extension UI dialog", slog.String("method", ev.Method))
		}
		// Fire-and-forget methods (notify, setStatus, ...) are dropped.

	default:
		p.mu.Lock()
		for _, sub := range p.subs {
			sub.push(ev)
		}
		p.mu.Unlock()
	}
}

func (p *process) write(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode pi command: %w", err)
	}
	data = append(data, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if _, err := p.stdin.Write(data); err != nil {
		return fmt.Errorf("write pi command: %w", err)
	}
	return nil
}

// call sends one command and waits for its correlated response.
func (p *process) call(ctx context.Context, cmd map[string]any) (response, error) {
	p.mu.Lock()
	if p.exitErr != nil {
		err := p.exitErr
		p.mu.Unlock()
		return response{}, err
	}
	p.nextID++
	id := "bb-" + strconv.FormatInt(p.nextID, 10)
	ch := make(chan response, 1)
	p.pending[id] = ch
	p.mu.Unlock()

	cmd["id"] = id
	if err := p.write(cmd); err != nil {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return response{}, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return response{}, p.exitError()
		}
		return resp, nil
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return response{}, ctx.Err()
	case <-p.done:
		return response{}, p.exitError()
	}
}

// callOK is call plus the success check, returning the response data.
func (p *process) callOK(ctx context.Context, cmd map[string]any) (json.RawMessage, error) {
	name, _ := cmd["type"].(string)
	resp, err := p.call(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("pi %s: %w", name, err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("pi %s failed: %s", name, resp.Error)
	}
	return resp.Data, nil
}

func (p *process) exitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitErr != nil {
		return p.exitErr
	}
	return errProcessExited
}

func (p *process) subscribe() *subscription {
	sub := newSubscription()
	p.mu.Lock()
	if p.exitErr != nil {
		p.mu.Unlock()
		sub.close()
		return sub
	}
	p.nextSub++
	id := p.nextSub
	p.subs[id] = sub
	sub.unsubscribe = func() {
		p.mu.Lock()
		delete(p.subs, id)
		p.mu.Unlock()
		sub.close()
	}
	p.mu.Unlock()
	return sub
}

// stop terminates the child: SIGTERM, then SIGKILL after a grace period.
func (p *process) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(processStopGrace):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// subscription is an unbounded event queue with a channel front-end, so the
// stdout read loop never blocks on a slow consumer and a cancelled consumer
// never strands the pump goroutine.
type subscription struct {
	mu          sync.Mutex
	queue       []rawEvent
	signal      chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	out         chan rawEvent
	unsubscribe func()
}

func newSubscription() *subscription {
	s := &subscription{
		signal: make(chan struct{}, 1),
		done:   make(chan struct{}),
		out:    make(chan rawEvent),
	}
	go s.pump()
	return s
}

// Events returns the receive side. It is closed when the subscription ends.
func (s *subscription) Events() <-chan rawEvent { return s.out }

// Cancel detaches from the process and ends the pump; queued events are
// discarded.
func (s *subscription) Cancel() {
	if s.unsubscribe != nil {
		s.unsubscribe()
		return
	}
	s.close()
}

func (s *subscription) push(ev rawEvent) {
	s.mu.Lock()
	s.queue = append(s.queue, ev)
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *subscription) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *subscription) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.signal:
				continue
			case <-s.done:
				return
			}
		}
		ev := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()

		select {
		case s.out <- ev:
		case <-s.done:
			return
		}
	}
}
