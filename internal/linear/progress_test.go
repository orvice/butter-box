package linear

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProgressActivity(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		payload   string
		want      activity
		ok        bool
	}{
		{"bash", "tool_execution_start", `{"toolName":"bash","args":{"command":"go   test\n./..."}}`, activity{Type: "action", Action: "Running", Parameter: "go test ./..."}, true},
		{"read", "tool_execution_start", `{"toolName":"read","args":{"path":"main.go"}}`, activity{Type: "action", Action: "Reading", Parameter: "main.go"}, true},
		{"edit", "tool_execution_start", `{"toolName":"edit","args":{"path":"a.go","oldText":"x","newText":"y"}}`, activity{Type: "action", Action: "Editing", Parameter: "a.go"}, true},
		{"grep", "tool_execution_start", `{"toolName":"grep","args":{"pattern":"TODO","path":"internal"}}`, activity{Type: "action", Action: "Searching", Parameter: "TODO in internal"}, true},
		{"ls default", "tool_execution_start", `{"toolName":"ls","args":{}}`, activity{Type: "action", Action: "Listing", Parameter: "."}, true},
		{"custom tool", "tool_execution_start", `{"toolName":"web_fetch","args":{"url":"https://example.com"}}`, activity{Type: "action", Action: "Using web_fetch", Parameter: "https://example.com"}, true},
		{"bare custom tool", "tool_execution_start", `{"toolName":"plan","args":{"steps":3}}`, activity{Type: "action", Action: "Using", Parameter: "plan"}, true},
		{"retry", "auto_retry_start", `{"attempt":2,"maxAttempts":3}`, activity{Type: "thought", Body: "The model call failed; retrying (attempt 2 of 3).", Ephemeral: true}, true},
		{"compaction", "compaction_start", `{"reason":"threshold"}`, activity{Type: "thought", Body: "Compacting the conversation context.", Ephemeral: true}, true},
		{"tool without name", "tool_execution_start", `{"args":{}}`, activity{}, false},
		{"streaming delta", "message_update", `{}`, activity{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := progressActivity(tt.eventType, []byte(tt.payload))
			if ok != tt.ok || got != tt.want {
				t.Fatalf("progressActivity = %+v, %v; want %+v, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestProgressParameterTruncated(t *testing.T) {
	payload := `{"toolName":"bash","args":{"command":"echo ` + strings.Repeat("a", 500) + `"}}`
	got, _ := progressActivity("tool_execution_start", []byte(payload))
	if n := len([]rune(got.Parameter)); n != maxParameterRunes || !strings.HasSuffix(got.Parameter, "…") {
		t.Fatalf("parameter has %d runes: %q", n, got.Parameter)
	}
}

func TestRedact(t *testing.T) {
	tests := []struct{ in, leak string }{
		{"curl -H 'Authorization: Bearer abc.def-123456'", "abc.def-123456"},
		{"GITHUB_TOKEN=ghp_abcdefghijklmnopqrstuvwxyz0123 gh pr list", "ghp_abcdefghijklmnopqrstuvwxyz"},
		{`export API_KEY="s3cr3t value"`, "s3cr3t"},
		{"mysql --password=hunter22 -u root", "hunter22"},
		{"git clone https://user:pa55word@example.com/repo.git", "pa55word"},
		{"key sk-ant-api03-abcdefghijklmnop", "abcdefghijklmnop"},
		{"token lin_api_abcdefghijklmnopqrstuvwxyz", "abcdefghijklmnopqrstuvwxyz"},
		{"aws AKIAABCDEFGHIJKLMNOP", "ABCDEFGHIJKLMNOP"},
	}
	for _, tt := range tests {
		got := redact(tt.in)
		if strings.Contains(got, tt.leak) {
			t.Errorf("redact(%q) = %q leaks %q", tt.in, got, tt.leak)
		}
	}
	if plain := "go test ./... && git status"; redact(plain) != plain {
		t.Errorf("redact changed harmless text: %q", redact(plain))
	}
}

func TestProgressCoalescesAndDeduplicates(t *testing.T) {
	var mu sync.Mutex
	var sent []activity
	release := make(chan struct{})
	first := true
	p := newProgress(func(a activity) {
		mu.Lock()
		sent = append(sent, a)
		wait := first
		first = false
		mu.Unlock()
		if wait {
			<-release // hold the worker so later updates pile up
		}
	}, time.Millisecond)

	p.offer(activity{Type: "action", Action: "Running", Parameter: "one"})
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(sent) == 1 })
	p.offer(activity{Type: "action", Action: "Running", Parameter: "two"})
	p.offer(activity{Type: "action", Action: "Running", Parameter: "three"})
	p.offer(activity{Type: "action", Action: "Running", Parameter: "three"})
	close(release)
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(sent) == 2 })
	p.close()

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 2 || sent[1].Parameter != "three" {
		t.Fatalf("sent = %+v, want one then the latest", sent)
	}
}

func TestProgressCloseDropsPending(t *testing.T) {
	var mu sync.Mutex
	var sent []activity
	p := newProgress(func(a activity) {
		mu.Lock()
		sent = append(sent, a)
		mu.Unlock()
	}, time.Hour)

	p.offer(activity{Type: "action", Action: "Running", Parameter: "one"})
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(sent) == 1 })
	p.offer(activity{Type: "action", Action: "Running", Parameter: "two"}) // waits out the interval
	p.close()

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("sent = %+v, want the pending update dropped on close", sent)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
