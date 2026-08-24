package pi

import (
	"strings"
	"testing"
)

func TestScanLinesStrictJSONL(t *testing.T) {
	// First record contains a literal U+2028 (a Unicode line separator that
	// generic line readers wrongly split on) inside a JSON string; second
	// record uses CRLF framing. Both must decode as exactly two records.
	input := "{\"type\":\"a\",\"text\":\"line sep\"}\n{\"type\":\"b\"}\r\n"

	var lines []string
	err := scanLines(strings.NewReader(input), func(line []byte) error {
		lines = append(lines, string(line))
		return nil
	})
	if err != nil {
		t.Fatalf("scanLines: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d records, want 2: %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "line sep") {
		t.Fatalf("U+2028 was mangled: %q", lines[0])
	}
	if lines[1] != `{"type":"b"}` {
		t.Fatalf("CR not stripped: %q", lines[1])
	}

	ev, err := decodeRaw([]byte(lines[0]))
	if err != nil {
		t.Fatalf("decodeRaw: %v", err)
	}
	if ev.Type != "a" {
		t.Fatalf("type = %q", ev.Type)
	}
}

func TestDecodeRawRejectsMissingType(t *testing.T) {
	if _, err := decodeRaw([]byte(`{"id":"x"}`)); err == nil {
		t.Fatal("expected error for record without type")
	}
}
