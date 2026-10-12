package pi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// entriesFixture replays a get_entries response at the external Pi boundary.
func entriesFixture(t *testing.T, payload string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "entries.json")
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PI_ENTRIES_DATA", path)
}

func TestTurnContentContracts(t *testing.T) {
	const answer = `{"type":"message","id":"answer","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"world"}]}}`
	cases := []struct {
		name     string
		entries  string
		text     string
		noResult bool
		wantErr  string
	}{
		{name: "user string", entries: `{"type":"message","message":{"role":"user","content":"hello"}},` + answer, text: "world"},
		{name: "user array", entries: `{"type":"message","message":{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image","data":"AA==","mimeType":"image/png"}]}},` + answer, text: "world"},
		{name: "system string", entries: `{"type":"message","message":{"role":"system","content":"instructions"}},` + answer, text: "world"},
		{name: "system array", entries: `{"type":"message","message":{"role":"system","content":[{"type":"text","text":"instructions"}]}},` + answer, text: "world"},
		{name: "custom string", entries: `{"type":"message","message":{"role":"custom","content":"context"}},` + answer, text: "world"},
		{name: "custom array", entries: `{"type":"message","message":{"role":"custom","content":[]}},` + answer, text: "world"},
		{name: "custom entry", entries: `{"type":"custom_message","content":"context"},` + answer, text: "world"},
		{name: "tool result", entries: `{"type":"message","message":{"role":"toolResult","content":[{"type":"text","text":"output"}]}},` + answer, text: "world"},
		{name: "bash execution", entries: `{"type":"message","message":{"role":"bashExecution","output":"output"}},` + answer, text: "world"},
		{name: "unknown role", entries: `{"type":"message","message":{"role":"extension","content":{"opaque":true},"stopReason":{"opaque":true}}},` + answer, text: "world"},
		{name: "non message entry", entries: `{"type":"custom","message":42},` + answer, text: "world"},
		{name: "last assistant wins", entries: answer + `,{"type":"message","message":{"role":"assistant","stopReason":"length","content":[{"type":"text","text":"new"},{"type":"text","text":" answer"}]}}`, text: "new answer"},
		{name: "opaque non text blocks", entries: `{"type":"message","message":{"role":"assistant","stopReason":"stop","content":[{"type":"thinking","thinking":"hmm"},{"type":"toolCall","arguments":{"x":1}},{"type":"extension","text":{"opaque":true}},{"type":"text","text":"world"}]}}`, text: "world"},
		{name: "only non text", entries: `{"type":"message","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm"}]}}`},
		{name: "empty array", entries: `{"type":"message","message":{"role":"assistant","content":[]}}`},
		{name: "null content", entries: `{"type":"message","message":{"role":"assistant","content":null}}`},
		{name: "missing content", entries: `{"type":"message","message":{"role":"assistant"}}`},
		{name: "no assistant", entries: `{"type":"message","message":{"role":"user","content":"hello"}}`, noResult: true},
		{name: "assistant string is invalid", entries: `{"type":"message","message":{"role":"assistant","content":"not an array"}}`, wantErr: "decode assistant content"},
		{name: "assistant object is invalid", entries: `{"type":"message","message":{"role":"assistant","content":{}}}`, wantErr: "decode assistant content"},
		{name: "assistant number is invalid", entries: `{"type":"message","message":{"role":"assistant","content":42}}`, wantErr: "decode assistant content"},
		{name: "invalid text", entries: `{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":42}]}}`, wantErr: "decode assistant content"},
		{name: "invalid block", entries: `{"type":"message","message":{"role":"assistant","content":[42]}}`, wantErr: "decode assistant content"},
		{name: "invalid earlier assistant", entries: `{"type":"message","message":{"role":"assistant","content":42}},` + answer, wantErr: "decode assistant content"},
		{name: "invalid entry", entries: `42,` + answer, wantErr: "decode session entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entriesFixture(t, `{"entries":[`+tc.entries+`],"leafId":"answer"}`)
			m := newFakeManager(t)
			ctx := testCtx(t)
			info, err := m.Create(ctx, CreateOpts{})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			status, err := m.Turn(ctx, info.ID, "", 0)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Turn error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Turn: %v", err)
			}
			if status.Running || (status.Result == nil) != tc.noResult {
				t.Fatalf("status = %+v", status)
			}
			if tc.noResult {
				return
			}
			if status.Result.Text != tc.text || status.Result.Stats.Input != 100 {
				t.Fatalf("result = %+v, want text %q and stats", status.Result, tc.text)
			}
			if tc.name == "last assistant wins" && status.Result.StopReason != "length" {
				t.Fatalf("stop reason = %q, want length", status.Result.StopReason)
			}
		})
	}
}

func TestEntriesPreserveOpaqueContent(t *testing.T) {
	const raw = `{ "type":"message", "id":"user-1", "message":{"role":"user","content":"hello","extension":{"nested":[1,true,null]}} }`
	entriesFixture(t, `{"entries":[`+raw+`],"leafId":"user-1"}`)
	m := newFakeManager(t)
	ctx := testCtx(t)
	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	result, err := m.Entries(ctx, info.ID, "")
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if result.Running || result.LeafID != "user-1" || len(result.Entries) != 1 {
		t.Fatalf("entries result = %+v", result)
	}
	entry := result.Entries[0]
	if entry.ID != "user-1" || entry.Type != "message" || string(entry.Raw) != raw {
		t.Fatalf("entry = %+v, want verbatim %s", entry, raw)
	}
}

func TestEntriesInvalidPayload(t *testing.T) {
	for _, payload := range []string{
		`{"entries":{}}`,
		`{"entries":[42]}`,
		`{"entries":[{"id":42,"type":"message"}]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			entriesFixture(t, payload)
			m := newFakeManager(t)
			ctx := testCtx(t)
			info, err := m.Create(ctx, CreateOpts{})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			if _, err := m.Entries(ctx, info.ID, ""); err == nil || !strings.Contains(err.Error(), "decode") {
				t.Fatalf("Entries error = %v, want decoding error", err)
			}
			if _, err := m.Turn(ctx, info.ID, "", 0); err == nil || !strings.Contains(err.Error(), "decode") {
				t.Fatalf("Turn error = %v, want decoding error", err)
			}
			if _, err := m.Submit(ctx, info.ID, "next", nil); err == nil || !strings.Contains(err.Error(), "decode") {
				t.Fatalf("Submit error = %v, want decoding error", err)
			}
		})
	}
}

func TestSubmitWithOpaqueEntryContent(t *testing.T) {
	entriesFixture(t, `{"entries":[{"type":"message","id":"user-1","message":{"role":"user","content":"hello","stopReason":{"extension":"opaque"}}}],"leafId":"user-1"}`)
	m := newFakeManager(t)
	ctx := testCtx(t)
	info, err := m.Create(ctx, CreateOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cursor, err := m.Submit(ctx, info.ID, "next", nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if cursor != "user-1" {
		t.Fatalf("cursor = %q, want user-1", cursor)
	}
}
