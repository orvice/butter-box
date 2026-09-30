package linear

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/orvice/butter-box/internal/pi"
)

const (
	defaultProgressInterval = 3 * time.Second
	postTimeout             = 15 * time.Second
	stopTimeout             = 10 * time.Second
)

// Agent is the pi session runtime Linear sessions are backed by;
// *pi.Manager implements it.
type Agent interface {
	Create(ctx context.Context, opts pi.CreateOpts) (pi.Info, error)
	Send(ctx context.Context, id, message string, images []pi.ImageInput, onEvent func(eventType string, payload []byte) error) (pi.SendResult, error)
	Abort(ctx context.Context, id string) error
}

// postFunc delivers one activity to a Linear agent session.
type postFunc func(ctx context.Context, organizationID, agentSessionID string, a activity) error

// runner backs each Linear agent session with one pi session and runs the
// session's prompts one at a time: a message that arrives mid-run is queued
// and delivered, in the same pi session, once the run settles.
type runner struct {
	agent            Agent
	post             postFunc
	sessions         *fileStore[sessionRecord]
	cfg              Config
	logger           *slog.Logger
	now              func() time.Time
	progressInterval time.Duration

	mu      sync.Mutex
	active  map[string]*runState // keyed by Linear agent session ID
	closing bool
	wg      sync.WaitGroup
}

// runState is one Linear session's run loop. It exists while a run is in
// flight or queued; all fields are guarded by runner.mu.
type runState struct {
	organizationID string
	issue          string
	pending        []string
	// stopped is set by a stop request and suppresses the stopped run's
	// result; the loop clears it before starting a queued prompt.
	stopped bool
	cancel  context.CancelFunc // non-nil while a run is in flight
	runDone chan struct{}      // closed once the in-flight run posts nothing more
}

func (r *runner) handle(ev sessionEvent) {
	id := ev.AgentSession.ID
	if id == "" {
		r.logger.Warn("linear agent session event without a session id", slog.String("action", ev.Action))
		return
	}
	org := ev.OrganizationID
	if org == "" {
		if rec, ok := r.sessions.get(id); ok {
			org = rec.OrganizationID
		}
	}

	switch ev.Action {
	case "created":
		// Linear marks a session unresponsive when no activity follows its
		// creation within seconds, so acknowledge before pi starts.
		ack := "Picked this up. Starting pi on ButterBox."
		if label := ev.issueLabel(); label != "" {
			ack = fmt.Sprintf("Picked up %s. Starting pi on ButterBox.", label)
		}
		r.notify(org, id, thought(ack))
		r.enqueue(org, id, ev.issueLabel(), initialPrompt(ev))
	case "prompted":
		if ev.isStop() {
			r.stop(org, id)
			return
		}
		_, known := r.sessions.get(id)
		if r.enqueue(org, id, ev.issueLabel(), followUpPrompt(ev, !known)) {
			r.notify(org, id, thought("Got it. This will run after the current task finishes."))
		}
	default:
		r.logger.Debug("ignoring linear agent session event", slog.String("action", ev.Action))
	}
}

// enqueue starts a run loop for the session, or queues prompt behind the
// run in flight. It reports whether prompt was queued.
func (r *runner) enqueue(organizationID, id, issue, prompt string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		r.logger.Warn("linear prompt dropped: shutting down", slog.String("agent_session_id", id))
		return false
	}
	if st, ok := r.active[id]; ok {
		st.pending = append(st.pending, prompt)
		return true
	}
	st := &runState{organizationID: organizationID, issue: issue}
	r.active[id] = st
	r.wg.Add(1)
	go r.loop(id, st, prompt)
	return false
}

func (r *runner) loop(id string, st *runState, prompt string) {
	defer r.wg.Done()
	for {
		r.runOnce(id, st, prompt)

		r.mu.Lock()
		if r.closing || len(st.pending) == 0 {
			delete(r.active, id)
			r.mu.Unlock()
			return
		}
		prompt = strings.Join(st.pending, "\n\n")
		st.pending = nil
		st.stopped = false
		r.mu.Unlock()
	}
}

func (r *runner) runOnce(id string, st *runState, prompt string) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RunTimeout)
	defer cancel()

	runDone := make(chan struct{})
	defer close(runDone)
	r.mu.Lock()
	if st.stopped || r.closing {
		r.mu.Unlock()
		return
	}
	st.cancel = cancel
	st.runDone = runDone
	org := st.organizationID
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		st.cancel = nil
		r.mu.Unlock()
	}()

	started := r.now()
	piID, err := r.ensureSession(ctx, id, st)
	if err != nil {
		r.finish(ctx, id, st, started, pi.SendResult{}, err)
		return
	}
	r.setRunning(id, true)

	report := newProgress(func(a activity) {
		if !r.isStopped(st) {
			r.notify(org, id, a)
		}
	}, r.progressInterval)
	result, err := r.agent.Send(ctx, piID, prompt, nil, report.observe)
	report.close()
	r.finish(ctx, id, st, started, result, err)
}

// finish reports a run's outcome to Linear.
func (r *runner) finish(ctx context.Context, id string, st *runState, started time.Time, result pi.SendResult, err error) {
	r.mu.Lock()
	stopped, closing, org := st.stopped, r.closing, st.organizationID
	r.mu.Unlock()
	if closing {
		// The box is shutting down: keep the running mark so the next
		// start tells Linear the run was cut short.
		return
	}
	r.setRunning(id, false)
	if stopped {
		return // the stop request already answered
	}

	elapsed := formatElapsed(r.now().Sub(started))
	var out activity
	switch {
	case err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		out = failure(fmt.Sprintf("pi timed out after %s and was stopped.", elapsed))
	case errors.Is(err, pi.ErrNotFound):
		if delErr := r.sessions.delete(id); delErr != nil {
			r.logger.Warn("drop linear session mapping", slog.String("agent_session_id", id), slog.Any("error", delErr))
		}
		out = failure("The pi session behind this Linear session no longer exists on ButterBox. Send another message to start a fresh one.")
	case errors.Is(err, pi.ErrTooManySessions):
		out = failure("ButterBox is at its pi session limit. Try again once another session goes idle.")
	case errors.Is(err, pi.ErrBusy):
		out = failure("The pi session is busy with a request from another client. Try again once it finishes.")
	case err != nil:
		out = failure(fmt.Sprintf("pi failed after %s: %s", elapsed, err))
	case result.StopReason == "aborted":
		out = failure(fmt.Sprintf("The pi run was aborted after %s.", elapsed))
	case result.StopReason == "error":
		body := fmt.Sprintf("pi stopped with an error after %s.", elapsed)
		if text := strings.TrimSpace(result.Text); text != "" {
			body += "\n\n" + text
		}
		out = failure(body)
	default:
		out = response(finalBody(result.Text, elapsed))
	}
	r.notify(org, id, out)
}

// ensureSession returns the pi session backing the Linear session, creating
// and recording one on first use.
func (r *runner) ensureSession(ctx context.Context, id string, st *runState) (string, error) {
	if rec, ok := r.sessions.get(id); ok {
		return rec.PiSessionID, nil
	}
	name := "Linear session"
	if st.issue != "" {
		name = "Linear " + st.issue
	}
	info, err := r.agent.Create(ctx, pi.CreateOpts{
		Name:          name,
		Provider:      r.cfg.Provider,
		Model:         r.cfg.Model,
		ThinkingLevel: r.cfg.ThinkingLevel,
		Cwd:           r.cfg.Cwd,
	})
	if err != nil {
		return "", err
	}
	now := r.now()
	rec := sessionRecord{
		PiSessionID:    info.ID,
		OrganizationID: st.organizationID,
		Issue:          st.issue,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	// Without the mapping the next message would lose this session's
	// history, so a failed write fails the run.
	if err := r.sessions.put(id, rec); err != nil {
		return "", err
	}
	r.logger.Info("pi session created for linear session",
		slog.String("agent_session_id", id),
		slog.String("pi_session_id", info.ID),
		slog.String("cwd", info.Cwd),
	)
	return info.ID, nil
}

// stop halts the session's run and drops its queue, then confirms to Linear
// once the stopped run can post nothing more.
func (r *runner) stop(organizationID, id string) {
	r.mu.Lock()
	st, running := r.active[id]
	var cancel context.CancelFunc
	var runDone chan struct{}
	if running {
		st.pending = nil
		st.stopped = true
		cancel, runDone = st.cancel, st.runDone
	}
	r.mu.Unlock()

	if !running {
		r.notify(organizationID, id, response("Nothing is running, so there is nothing to stop."))
		return
	}
	r.logger.Info("stopping linear session run", slog.String("agent_session_id", id))
	if cancel != nil {
		cancel() // Send aborts the pi run on cancellation
	}
	// A prompt racing the cancel may already have reached pi: abort it too.
	if rec, ok := r.sessions.get(id); ok {
		ctx, done := context.WithTimeout(context.Background(), stopTimeout)
		if err := r.agent.Abort(ctx, rec.PiSessionID); err != nil {
			r.logger.Warn("abort pi session", slog.String("pi_session_id", rec.PiSessionID), slog.Any("error", err))
		}
		done()
	}
	if runDone != nil {
		select {
		case <-runDone:
		case <-time.After(stopTimeout):
			r.logger.Warn("stopped pi run did not wind down in time", slog.String("agent_session_id", id))
		}
	}
	r.notify(organizationID, id, response("Stopped. Send a message to continue in the same pi session."))
}

func (r *runner) isStopped(st *runState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return st.stopped
}

func (r *runner) setRunning(id string, running bool) {
	now := r.now()
	err := r.sessions.update(id, func(rec *sessionRecord) {
		rec.Running = running
		rec.UpdatedAt = now
	})
	if err != nil {
		r.logger.Warn("record linear run state", slog.String("agent_session_id", id), slog.Any("error", err))
	}
}

// interrupted clears and returns the sessions whose run a previous process
// left unfinished.
func (r *runner) interrupted() map[string]sessionRecord {
	out := map[string]sessionRecord{}
	for id, rec := range r.sessions.all() {
		if rec.Running {
			out[id] = rec
			r.setRunning(id, false)
		}
	}
	return out
}

// close stops new runs from starting. Runs in flight end when their pi
// processes do; they keep their running mark for the next start to report.
func (r *runner) close() {
	r.mu.Lock()
	r.closing = true
	r.mu.Unlock()
}

// notify posts a to Linear, logging rather than failing: a lost progress
// update must never break a run.
func (r *runner) notify(organizationID, id string, a activity) {
	if a.Body != "" {
		a.Body = truncate(redact(a.Body), maxBodyRunes)
	}
	ctx, cancel := context.WithTimeout(context.Background(), postTimeout)
	defer cancel()
	if err := r.post(ctx, organizationID, id, a); err != nil {
		r.logger.Warn("post linear activity failed",
			slog.String("agent_session_id", id),
			slog.String("type", a.Type),
			slog.Any("error", err),
		)
	}
}

const preamble = `You are pi, working as a Linear agent on ButterBox.
Work in the repository in your working directory and make code changes when the task calls for it.
Never reveal secrets or credentials.
Your final message is posted to Linear as your reply: keep it concise and cover what changed, the checks you ran, and any follow-up.`

// initialPrompt opens a session with Linear's context for it.
func initialPrompt(ev sessionEvent) string {
	var b strings.Builder
	b.WriteString(preamble)
	if pc := strings.TrimSpace(ev.PromptContext); pc != "" {
		b.WriteString("\n\nLinear context:\n")
		b.WriteString(pc)
	} else if issue := ev.AgentSession.Issue; issue != nil {
		b.WriteString("\n\nLinear issue:")
		for _, field := range [][2]string{
			{"Identifier", issue.Identifier},
			{"Title", issue.Title},
			{"URL", issue.URL},
		} {
			if field[1] != "" {
				fmt.Fprintf(&b, "\n- %s: %s", field[0], field[1])
			}
		}
		if d := strings.TrimSpace(issue.Description); d != "" {
			b.WriteString("\n- Description:\n")
			b.WriteString(d)
		}
	}
	return b.String()
}

// followUpPrompt carries a user message; fresh sessions also get the
// session's context, since pi has no history to draw on.
func followUpPrompt(ev sessionEvent, fresh bool) string {
	msg := ev.message()
	if msg == "" {
		msg = "(The message has no text. Continue and report your status.)"
	}
	if fresh {
		return initialPrompt(ev) + "\n\nLatest message from the Linear user:\n" + msg
	}
	return "Follow-up from the Linear user:\n" + msg
}

// finalBody is the run's reply with its duration, within Linear's size cap.
func finalBody(text, elapsed string) string {
	footer := fmt.Sprintf("\n\n_Finished in %s._", elapsed)
	text = strings.TrimSpace(text)
	if text == "" {
		text = "pi finished without a text reply."
	}
	return truncate(redact(text), maxBodyRunes-len([]rune(footer))) + footer
}

// formatElapsed renders a duration for people: "45s", "3m 12s", "1h 5m".
func formatElapsed(d time.Duration) string {
	d = d.Round(time.Second)
	h, m, s := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	case m > 0:
		return fmt.Sprintf("%dm %ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}
