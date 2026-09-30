package linear

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orvice/butter-box/internal/pi"
)

func TestCreatedEventRunsPiAndReplies(t *testing.T) {
	agent := newFakeAgent()
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))

	ack := rec.next(t, "thought")
	if !strings.Contains(ack.Body, "ENG-1: Fix login") || ack.Org != "org-1" || ack.Session != "ls-1" {
		t.Fatalf("ack = %+v", ack)
	}
	reply := rec.next(t, "response")
	if !strings.HasPrefix(reply.Body, "done: ") || !strings.Contains(reply.Body, "_Finished in ") {
		t.Fatalf("reply = %q", reply.Body)
	}
	i.runner.wg.Wait()

	creates, sends, _ := agent.snapshot()
	if len(creates) != 1 || creates[0].Cwd != "repo" || creates[0].Name != "Linear ENG-1: Fix login" {
		t.Fatalf("creates = %+v", creates)
	}
	if len(sends) != 1 || sends[0].ID != "pi-1" {
		t.Fatalf("sends = %+v", sends)
	}
	if !strings.Contains(sends[0].Message, "working as a Linear agent") || !strings.Contains(sends[0].Message, `<issue identifier="ENG-1">`) {
		t.Fatalf("prompt = %q", sends[0].Message)
	}

	mapping, ok := i.runner.sessions.get("ls-1")
	if !ok || mapping.PiSessionID != "pi-1" || mapping.Running || mapping.OrganizationID != "org-1" {
		t.Fatalf("mapping = %+v, %v", mapping, ok)
	}
}

func TestFollowUpReusesPiSession(t *testing.T) {
	agent := newFakeAgent()
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	rec.next(t, "response")
	i.runner.wg.Wait()

	i.dispatch(promptedEvent("ls-1", "Also update the docs", ""))
	rec.next(t, "response")
	i.runner.wg.Wait()

	creates, sends, _ := agent.snapshot()
	if len(creates) != 1 {
		t.Fatalf("creates = %d, want one pi session", len(creates))
	}
	if len(sends) != 2 || sends[1].ID != "pi-1" {
		t.Fatalf("sends = %+v", sends)
	}
	if sends[1].Message != "Follow-up from the Linear user:\nAlso update the docs" {
		t.Fatalf("follow-up prompt = %q", sends[1].Message)
	}
}

func TestPromptForUnknownSessionCarriesContext(t *testing.T) {
	agent := newFakeAgent()
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	ev := promptedEvent("ls-new", "Please continue", "")
	ev.PromptContext = "issue context"
	i.dispatch(ev)
	rec.next(t, "response")
	i.runner.wg.Wait()

	_, sends, _ := agent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("sends = %+v", sends)
	}
	msg := sends[0].Message
	if !strings.Contains(msg, "working as a Linear agent") || !strings.Contains(msg, "issue context") || !strings.HasSuffix(msg, "Please continue") {
		t.Fatalf("prompt = %q", msg)
	}
}

func TestPromptDuringRunIsQueued(t *testing.T) {
	agent := newFakeAgent()
	release := make(chan struct{})
	agent.sendFn = func(_ context.Context, message string, _ func(string, []byte) error) (pi.SendResult, error) {
		if strings.Contains(message, "working as a Linear agent") {
			<-release
		}
		return pi.SendResult{Text: "done", StopReason: "stop"}, nil
	}
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	agent.waitStarted(t)
	i.dispatch(promptedEvent("ls-1", "first extra", ""))
	if q := rec.next(t, "thought"); !strings.Contains(q.Body, "Picked up") {
		t.Fatalf("first thought = %q, want the ack", q.Body)
	}
	if q := rec.next(t, "thought"); !strings.Contains(q.Body, "after the current task") {
		t.Fatalf("queued thought = %q", q.Body)
	}
	i.dispatch(promptedEvent("ls-1", "second extra", ""))
	rec.next(t, "thought")
	close(release)

	rec.next(t, "response")
	rec.next(t, "response")
	i.runner.wg.Wait()

	_, sends, _ := agent.snapshot()
	if len(sends) != 2 {
		t.Fatalf("sends = %+v, want the queue delivered as one run", sends)
	}
	want := "Follow-up from the Linear user:\nfirst extra\n\nFollow-up from the Linear user:\nsecond extra"
	if sends[1].Message != want {
		t.Fatalf("queued prompt = %q", sends[1].Message)
	}
}

// blockUntilCancelled mimics pi.Manager.Send: a cancelled context aborts the
// run and surfaces as the error.
func blockUntilCancelled(ctx context.Context, _ string, _ func(string, []byte) error) (pi.SendResult, error) {
	<-ctx.Done()
	return pi.SendResult{}, ctx.Err()
}

func TestStopAbortsRunAndDropsQueue(t *testing.T) {
	agent := newFakeAgent()
	agent.sendFn = blockUntilCancelled
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	agent.waitStarted(t)
	i.dispatch(promptedEvent("ls-1", "queued behind", ""))
	rec.next(t, "thought") // ack
	rec.next(t, "thought") // queued

	i.dispatch(promptedEvent("ls-1", "", "stop"))
	stopped := rec.next(t, "response")
	if !strings.HasPrefix(stopped.Body, "Stopped.") {
		t.Fatalf("stop reply = %q", stopped.Body)
	}
	i.runner.wg.Wait()

	_, sends, aborts := agent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("sends = %+v, want the queued prompt dropped", sends)
	}
	if len(aborts) != 1 || aborts[0] != "pi-1" {
		t.Fatalf("aborts = %v", aborts)
	}
	for _, p := range rec.all() {
		if p.Type == "error" {
			t.Fatalf("stopped run reported an error: %+v", p)
		}
	}
	if mapping, _ := i.runner.sessions.get("ls-1"); mapping.Running {
		t.Fatal("stopped run still marked running")
	}
}

func TestStopWhenIdle(t *testing.T) {
	agent := newFakeAgent()
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(promptedEvent("ls-1", "", "stop"))
	if got := rec.next(t, "response"); !strings.Contains(got.Body, "Nothing is running") {
		t.Fatalf("reply = %q", got.Body)
	}
	if _, sends, aborts := agent.snapshot(); len(sends) != 0 || len(aborts) != 0 {
		t.Fatalf("sends = %v, aborts = %v", sends, aborts)
	}
}

func TestRunTimeout(t *testing.T) {
	agent := newFakeAgent()
	agent.sendFn = blockUntilCancelled
	cfg := testConfig(t)
	cfg.RunTimeout = 50 * time.Millisecond
	i := newTestIntegration(t, cfg, agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	if got := rec.next(t, "error"); !strings.Contains(got.Body, "timed out") {
		t.Fatalf("error = %q", got.Body)
	}
}

func TestRunErrorsReported(t *testing.T) {
	tests := []struct {
		name   string
		result pi.SendResult
		err    error
		want   string
	}{
		{"pi failure", pi.SendResult{}, errors.New("pi exited unexpectedly"), "pi failed after"},
		{"session limit", pi.SendResult{}, pi.ErrTooManySessions, "session limit"},
		{"busy", pi.SendResult{}, pi.ErrBusy, "busy"},
		{"model error", pi.SendResult{Text: "overloaded", StopReason: "error"}, nil, "pi stopped with an error"},
		{"aborted elsewhere", pi.SendResult{StopReason: "aborted"}, nil, "aborted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent := newFakeAgent()
			agent.sendFn = func(context.Context, string, func(string, []byte) error) (pi.SendResult, error) {
				return tt.result, tt.err
			}
			i := newTestIntegration(t, testConfig(t), agent, nil)
			rec := withRecorder(i)

			i.dispatch(createdEvent("ls-1"))
			if got := rec.next(t, "error"); !strings.Contains(got.Body, tt.want) {
				t.Fatalf("error = %q, want it to mention %q", got.Body, tt.want)
			}
		})
	}
}

func TestMissingPiSessionDropsMapping(t *testing.T) {
	agent := newFakeAgent()
	agent.sendFn = func(context.Context, string, func(string, []byte) error) (pi.SendResult, error) {
		return pi.SendResult{}, pi.ErrNotFound
	}
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	if got := rec.next(t, "error"); !strings.Contains(got.Body, "no longer exists") {
		t.Fatalf("error = %q", got.Body)
	}
	i.runner.wg.Wait()
	if _, ok := i.runner.sessions.get("ls-1"); ok {
		t.Fatal("mapping to a missing pi session was kept")
	}
}

func TestCreateFailureReported(t *testing.T) {
	agent := newFakeAgent()
	agent.createErr = pi.ErrInvalidCwd
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	if got := rec.next(t, "error"); !strings.Contains(got.Body, "invalid working directory") {
		t.Fatalf("error = %q", got.Body)
	}
}

func TestProgressPostedBeforeReply(t *testing.T) {
	agent := newFakeAgent()
	agent.sendFn = func(_ context.Context, _ string, onEvent func(string, []byte) error) (pi.SendResult, error) {
		_ = onEvent("agent_start", []byte(`{"type":"agent_start"}`))
		_ = onEvent("tool_execution_start", []byte(`{"type":"tool_execution_start","toolName":"bash","args":{"command":"curl -H 'Authorization: Bearer abcdefghijklmnop' https://api.example.com"}}`))
		time.Sleep(50 * time.Millisecond) // let the progress worker post
		return pi.SendResult{Text: "all done", StopReason: "stop"}, nil
	}
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	rec.next(t, "response")
	i.runner.wg.Wait()

	var types []string
	var action posted
	for _, p := range rec.all() {
		types = append(types, p.Type)
		if p.Type == "action" {
			action = p
		}
	}
	if strings.Join(types, ",") != "thought,action,response" {
		t.Fatalf("activity order = %v", types)
	}
	if action.Action != "Running" || strings.Contains(action.Parameter, "abcdefghijklmnop") || !strings.Contains(action.Parameter, "[redacted]") {
		t.Fatalf("action = %+v", action.activity)
	}
}

func TestStartReportsInterruptedRuns(t *testing.T) {
	cfg := testConfig(t)
	agent := newFakeAgent()
	first := newTestIntegration(t, cfg, agent, nil)
	now := time.Now()
	if err := first.runner.sessions.put("ls-1", sessionRecord{PiSessionID: "pi-1", OrganizationID: "org-1", Running: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	// A new process over the same state dir.
	second := newTestIntegration(t, cfg, agent, nil)
	rec := withRecorder(second)
	second.Start()

	got := rec.next(t, "error")
	if got.Session != "ls-1" || got.Org != "org-1" || !strings.Contains(got.Body, "restarted") {
		t.Fatalf("notice = %+v", got)
	}
	if mapping, _ := second.runner.sessions.get("ls-1"); mapping.Running || mapping.PiSessionID != "pi-1" {
		t.Fatalf("mapping after recovery = %+v", mapping)
	}
}

func TestShutdownKeepsRunningMark(t *testing.T) {
	agent := newFakeAgent()
	agent.sendFn = blockUntilCancelled
	i := newTestIntegration(t, testConfig(t), agent, nil)
	rec := withRecorder(i)

	i.dispatch(createdEvent("ls-1"))
	agent.waitStarted(t)
	i.Close()
	// What manager.Stop does to a run in flight.
	i.runner.mu.Lock()
	cancel := i.runner.active["ls-1"].cancel
	i.runner.mu.Unlock()
	cancel()
	i.runner.wg.Wait()

	if mapping, _ := i.runner.sessions.get("ls-1"); !mapping.Running {
		t.Fatal("run cut short by shutdown lost its running mark")
	}
	for _, p := range rec.all() {
		if p.Type != "thought" {
			t.Fatalf("shutdown posted %+v", p)
		}
	}
}

func TestFormatElapsed(t *testing.T) {
	for d, want := range map[time.Duration]string{
		4 * time.Second:                         "4s",
		3*time.Minute + 12*time.Second:          "3m 12s",
		time.Hour + 5*time.Minute + time.Second: "1h 5m",
	} {
		if got := formatElapsed(d); got != want {
			t.Errorf("formatElapsed(%s) = %q, want %q", d, got, want)
		}
	}
}
