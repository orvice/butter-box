package pi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain lets the test binary double as a fake pi executable: tests spawn
// os.Args[0] with FAKE_PI=1 and the fake speaks just enough of the RPC JSONL
// protocol for the manager to drive it.
func TestMain(m *testing.M) {
	if os.Getenv("FAKE_PI") == "1" {
		fakePiMain()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

const fakeSessionID = "fake-session-0001"

type fakePi struct {
	out        *bufio.Writer
	outMu      sync.Mutex
	sessionID  string
	lastPrompt string
	uiResp     chan map[string]any
	abortCh    chan struct{}
}

func fakePiMain() {
	f := &fakePi{
		out:       bufio.NewWriter(os.Stdout),
		sessionID: fakeSessionID,
		uiResp:    make(chan map[string]any, 1),
		abortCh:   make(chan struct{}, 1),
	}

	// `--session <id>`: pi silently starts a fresh session when the ID is
	// unknown; the fake adopts the ID only when it "exists".
	args := os.Args[1:]
	for i, a := range args {
		if a == "--session" && i+1 < len(args) {
			if strings.HasPrefix(args[i+1], "known-") {
				f.sessionID = args[i+1]
			}
		}
	}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	for sc.Scan() {
		var cmd map[string]any
		if err := json.Unmarshal(sc.Bytes(), &cmd); err != nil {
			continue
		}
		f.handle(cmd)
	}
}

func (f *fakePi) emit(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	f.outMu.Lock()
	defer f.outMu.Unlock()
	f.out.Write(data)
	f.out.WriteByte('\n')
	f.out.Flush()
}

func (f *fakePi) respond(cmd map[string]any, data any) {
	resp := map[string]any{
		"type":    "response",
		"id":      cmd["id"],
		"command": cmd["type"],
		"success": true,
	}
	if data != nil {
		resp["data"] = data
	}
	f.emit(resp)
}

func (f *fakePi) handle(cmd map[string]any) {
	switch cmd["type"] {
	case "get_state":
		f.respond(cmd, map[string]any{
			"model":        map[string]any{"id": "fake-model", "provider": "fake"},
			"isStreaming":  false,
			"sessionFile":  "/tmp/" + f.sessionID + ".jsonl",
			"sessionId":    f.sessionID,
			"sessionName":  "fake",
			"messageCount": 1,
		})
	case "set_thinking_level":
		f.respond(cmd, nil)
	case "get_last_assistant_text":
		f.respond(cmd, map[string]any{"text": "echo: " + f.lastPrompt})
	case "get_session_stats":
		f.respond(cmd, map[string]any{
			"tokens":       map[string]any{"input": 100, "output": 25, "cacheRead": 10, "cacheWrite": 5},
			"cost":         0.5,
			"contextUsage": map[string]any{"percent": 12.0},
		})
	case "abort":
		select {
		case f.abortCh <- struct{}{}:
		default:
		}
		f.respond(cmd, nil)
	case "extension_ui_response":
		f.uiResp <- cmd
	case "prompt":
		f.lastPrompt, _ = cmd["message"].(string)
		f.respond(cmd, nil)
		go f.run(f.lastPrompt)
	default:
		f.emit(map[string]any{
			"type":    "response",
			"id":      cmd["id"],
			"command": cmd["type"],
			"success": false,
			"error":   fmt.Sprintf("fake pi: unsupported command %v", cmd["type"]),
		})
	}
}

// run emits one agent run's event stream.
func (f *fakePi) run(prompt string) {
	f.emit(map[string]any{"type": "agent_start"})

	if strings.Contains(prompt, "dialog") {
		// Block on an extension UI confirm; the client must answer or the
		// run never settles.
		f.emit(map[string]any{
			"type":   "extension_ui_request",
			"id":     "ui-1",
			"method": "confirm",
			"title":  "Proceed?",
		})
		resp := <-f.uiResp
		if resp["cancelled"] != true {
			f.emit(map[string]any{"type": "agent_settled", "note": "unexpected ui answer"})
			return
		}
	}

	if strings.Contains(prompt, "slow") {
		select {
		case <-time.After(2 * time.Second):
		case <-f.abortCh:
			f.emit(map[string]any{"type": "agent_settled"})
			return
		}
	}

	f.emit(map[string]any{"type": "message_start", "message": map[string]any{"role": "assistant"}})
	f.emit(map[string]any{
		"type":                  "message_update",
		"assistantMessageEvent": map[string]any{"type": "text_delta", "contentIndex": 0, "delta": "echo: " + prompt},
	})
	f.emit(map[string]any{
		"type":    "message_end",
		"message": map[string]any{"role": "assistant", "stopReason": "stop"},
	})
	f.emit(map[string]any{"type": "turn_end"})
	f.emit(map[string]any{"type": "agent_end", "willRetry": false})
	f.emit(map[string]any{"type": "agent_settled"})
}

// newFakeManager returns a Manager that spawns this test binary as pi.
func newFakeManager(t *testing.T) *Manager {
	t.Helper()
	t.Setenv("FAKE_PI", "1")
	m := NewManager(testLogger(t), Config{Bin: os.Args[0], MaxSessions: 4})
	t.Cleanup(m.Stop)
	return m
}
