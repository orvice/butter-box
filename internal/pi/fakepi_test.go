package pi

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	entriesMu sync.Mutex
	entries   []map[string]any
	nextEntry int
}

// appendEntry records one session entry, mimicking pi's entry log.
func (f *fakePi) appendEntry(entry map[string]any) {
	f.entriesMu.Lock()
	defer f.entriesMu.Unlock()
	f.nextEntry++
	entry["id"] = fmt.Sprintf("entry-%04d", f.nextEntry)
	f.entries = append(f.entries, entry)
}

// entriesSince returns entries after the given id (all when since is nil),
// the current leaf id, and whether since was found.
func (f *fakePi) entriesSince(since any) ([]map[string]any, any, bool) {
	f.entriesMu.Lock()
	defer f.entriesMu.Unlock()
	var leaf any
	if n := len(f.entries); n > 0 {
		leaf = f.entries[n-1]["id"]
	}
	start := 0
	if since != nil {
		found := false
		for i, e := range f.entries {
			if e["id"] == since {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, leaf, false
		}
	}
	out := make([]map[string]any, len(f.entries)-start)
	copy(out, f.entries[start:])
	return out, leaf, true
}

func fakePiMain() {
	recordSpawn()

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
	case "get_entries":
		entries, leaf, ok := f.entriesSince(cmd["since"])
		if !ok {
			f.emit(map[string]any{
				"type":    "response",
				"id":      cmd["id"],
				"command": "get_entries",
				"success": false,
				"error":   fmt.Sprintf("Entry not found: %v", cmd["since"]),
			})
			return
		}
		f.respond(cmd, map[string]any{"entries": entries, "leafId": leaf})
	case "get_available_models":
		f.respond(cmd, map[string]any{
			"models": []map[string]any{
				{
					"id":            "fake-model",
					"provider":      "fake",
					"name":          "Fake Model",
					"api":           "anthropic-messages",
					"reasoning":     true,
					"input":         []string{"text", "image"},
					"contextWindow": 200000,
					"maxTokens":     64000,
					"cost":          map[string]any{"input": 3.0, "output": 15.0, "cacheRead": 0.3, "cacheWrite": 3.75},
				},
				{
					"id":            "fake-mini",
					"provider":      "fake",
					"name":          "Fake Mini",
					"api":           "openai-responses",
					"reasoning":     false,
					"input":         []string{"text"},
					"contextWindow": 128000,
					"maxTokens":     32000,
					"cost":          map[string]any{"input": 0.5, "output": 1.5, "cacheRead": 0.05, "cacheWrite": 0.0},
				},
			},
		})
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
	f.appendEntry(map[string]any{
		"type": "message",
		"message": map[string]any{
			"role":    "user",
			"content": []map[string]any{{"type": "text", "text": prompt}},
		},
	})

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

	f.appendEntry(map[string]any{
		"type": "message",
		"message": map[string]any{
			"role":       "assistant",
			"stopReason": "stop",
			"content": []map[string]any{
				{"type": "thinking", "thinking": "hmm"},
				{"type": "text", "text": "echo: " + prompt},
			},
		},
	})
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

// recordSpawn appends the fake's working directory and argv to the file named
// by FAKE_PI_SPAWN_LOG, so tests can assert the spawn directory.
func recordSpawn() {
	logPath := os.Getenv("FAKE_PI_SPAWN_LOG")
	if logPath == "" {
		return
	}
	cwd, _ := os.Getwd()
	rec, err := json.Marshal(map[string]any{"cwd": cwd, "args": os.Args[1:]})
	if err != nil {
		return
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(rec, '\n'))
}

// newFakeManager returns a Manager that spawns this test binary as pi.
func newFakeManager(t *testing.T) *Manager {
	t.Helper()
	return newFakeManagerCfg(t, Config{MaxSessions: 4})
}

// newFakeManagerCfg is newFakeManager with caller-controlled Config; Bin is
// always this test binary.
func newFakeManagerCfg(t *testing.T, cfg Config) *Manager {
	t.Helper()
	t.Setenv("FAKE_PI", "1")
	cfg.Bin = os.Args[0]
	m := NewManager(testLogger(t), cfg)
	t.Cleanup(m.Stop)
	return m
}

// spawnRecord is one fake pi launch, as recorded by recordSpawn.
type spawnRecord struct {
	Cwd  string   `json:"cwd"`
	Args []string `json:"args"`
}

// spawnLogSetup points FAKE_PI_SPAWN_LOG at a fresh file and returns a reader
// for the recorded spawns, in spawn order.
func spawnLogSetup(t *testing.T) func() []spawnRecord {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "spawns.jsonl")
	t.Setenv("FAKE_PI_SPAWN_LOG", logPath)
	return func() []spawnRecord {
		data, err := os.ReadFile(logPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil // nothing spawned yet
		}
		if err != nil {
			t.Fatalf("read spawn log: %v", err)
		}
		var records []spawnRecord
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var rec spawnRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("decode spawn record %q: %v", line, err)
			}
			records = append(records, rec)
		}
		return records
	}
}
