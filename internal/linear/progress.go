package linear

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// maxParameterRunes bounds the tool detail shown in a progress activity.
	maxParameterRunes = 200
	// maxBodyRunes bounds a final response or error body.
	maxBodyRunes = 8000
)

// progress turns pi's event stream into Linear activities without ever
// blocking it: observe only records the latest update, and one worker posts
// at most one activity per interval, dropping identical repeats.
type progress struct {
	send     func(activity)
	interval time.Duration

	mu      sync.Mutex
	pending *activity
	lastKey string

	wake   chan struct{}
	done   chan struct{}
	exited chan struct{}
}

func newProgress(send func(activity), interval time.Duration) *progress {
	p := &progress{
		send:     send,
		interval: interval,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
		exited:   make(chan struct{}),
	}
	go p.run()
	return p
}

// observe is a pi.Manager.Send event callback. It never fails the run.
func (p *progress) observe(eventType string, payload []byte) error {
	if a, ok := progressActivity(eventType, payload); ok {
		p.offer(a)
	}
	return nil
}

func (p *progress) offer(a activity) {
	key := a.Type + "\x00" + a.Action + "\x00" + a.Parameter + "\x00" + a.Body
	p.mu.Lock()
	if key == p.lastKey {
		p.mu.Unlock()
		return
	}
	p.lastKey = key
	p.pending = &a
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *progress) run() {
	defer close(p.exited)
	for {
		select {
		case <-p.done:
			return
		case <-p.wake:
		}
		p.mu.Lock()
		a := p.pending
		p.pending = nil
		p.mu.Unlock()
		if a == nil {
			continue
		}
		p.send(*a)
		select {
		case <-p.done:
			return
		case <-time.After(p.interval):
		}
	}
}

// close drops any unsent update and waits for an in-flight post, so nothing
// lands after the run's final activity.
func (p *progress) close() {
	close(p.done)
	<-p.exited
}

// toolEvent is the part of a tool_execution_start event we read.
type toolEvent struct {
	ToolName string          `json:"toolName"`
	Args     json.RawMessage `json:"args"`
}

type retryEvent struct {
	Attempt     int `json:"attempt"`
	MaxAttempts int `json:"maxAttempts"`
}

// progressActivity maps one pi event to a progress activity, if it merits one.
func progressActivity(eventType string, payload []byte) (activity, bool) {
	switch eventType {
	case "tool_execution_start":
		var ev toolEvent
		if json.Unmarshal(payload, &ev) != nil || ev.ToolName == "" {
			return activity{}, false
		}
		verb, detail := describeTool(ev.ToolName, ev.Args)
		return activity{Type: "action", Action: verb, Parameter: truncate(redact(detail), maxParameterRunes)}, true
	case "compaction_start":
		return activity{Type: "thought", Body: "Compacting the conversation context.", Ephemeral: true}, true
	case "auto_retry_start":
		var ev retryEvent
		_ = json.Unmarshal(payload, &ev)
		body := "The model call failed; retrying."
		if ev.Attempt > 0 && ev.MaxAttempts > 0 {
			body = fmt.Sprintf("The model call failed; retrying (attempt %d of %d).", ev.Attempt, ev.MaxAttempts)
		}
		return activity{Type: "thought", Body: body, Ephemeral: true}, true
	}
	return activity{}, false
}

// describeTool picks an action verb and a one-line target for a tool call.
func describeTool(name string, rawArgs json.RawMessage) (verb, detail string) {
	var args map[string]any
	_ = json.Unmarshal(rawArgs, &args)
	str := func(keys ...string) string {
		for _, k := range keys {
			if s, ok := args[k].(string); ok && strings.TrimSpace(s) != "" {
				return strings.Join(strings.Fields(s), " ")
			}
		}
		return ""
	}

	switch strings.ToLower(name) {
	case "bash":
		return "Running", orDefault(str("command", "cmd"), "a shell command")
	case "read":
		return "Reading", orDefault(str("path", "file_path"), "a file")
	case "edit":
		return "Editing", orDefault(str("path", "file_path"), "a file")
	case "write":
		return "Writing", orDefault(str("path", "file_path"), "a file")
	case "ls":
		return "Listing", orDefault(str("path"), ".")
	case "grep", "find":
		detail := str("pattern", "query")
		if where := str("path", "glob"); where != "" {
			detail = strings.TrimSpace(detail + " in " + where)
		}
		return "Searching", orDefault(detail, "the workspace")
	}
	if detail := str("path", "query", "url", "name", "title"); detail != "" {
		return "Using " + name, detail
	}
	return "Using", name
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// truncate shortens s to at most max runes, marking the cut.
func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max-1]) + "…"
}

type redaction struct {
	re   *regexp.Regexp
	repl string
}

// redactions scrub obvious credentials from text posted to Linear. They are
// best effort: the agent's workspace should not hold secrets it may not show.
var redactions = []redaction{
	{regexp.MustCompile(`(?i)(authorization:\s*(?:bearer|basic|token)\s+)\S+`), "${1}[redacted]"},
	{regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}`), "${1}[redacted]"},
	{regexp.MustCompile(`(?i)(--?(?:token|password|passwd|secret|api-?key|access-?key)(?:=|\s+))\S+`), "${1}[redacted]"},
	{regexp.MustCompile(`(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY)[A-Z0-9_]*\s*[=:]\s*)("[^"]*"|'[^']*'|\S+)`), "${1}[redacted]"},
	{regexp.MustCompile(`(https?://)[^/\s:@]+:[^/\s@]+@`), "${1}[redacted]@"},
	{regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{12,}`), "sk-[redacted]"},
	{regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}`), "gh_[redacted]"},
	{regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`), "github_pat_[redacted]"},
	{regexp.MustCompile(`\blin_(?:api|oauth)_[A-Za-z0-9]{20,}`), "lin_[redacted]"},
	{regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`), "xox-[redacted]"},
	{regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`), "AKIA[redacted]"},
}

func redact(s string) string {
	for _, r := range redactions {
		s = r.re.ReplaceAllString(s, r.repl)
	}
	return s
}
