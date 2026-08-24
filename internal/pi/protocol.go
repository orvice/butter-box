// Package pi drives `pi --mode rpc` child processes over the JSONL protocol
// documented in pi's docs/rpc.md, and exposes them as managed sessions.
package pi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// maxLineBytes bounds a single JSONL record. Prompts and events may embed
// base64 images, so this is generous.
const maxLineBytes = 64 << 20

// rawEvent is one decoded stdout line plus the fields every record carries.
type rawEvent struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Method string          `json:"method"`
	raw    json.RawMessage // full line, verbatim
}

// response is the decoded shape of a `type:"response"` record.
type response struct {
	ID      string          `json:"id"`
	Command string          `json:"command"`
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

// stateData mirrors the fields of get_state's response data we consume.
type stateData struct {
	Model *struct {
		ID       string `json:"id"`
		Provider string `json:"provider"`
	} `json:"model"`
	IsStreaming  bool   `json:"isStreaming"`
	SessionFile  string `json:"sessionFile"`
	SessionID    string `json:"sessionId"`
	SessionName  string `json:"sessionName"`
	MessageCount int32  `json:"messageCount"`
}

// statsData mirrors the fields of get_session_stats' response data we consume.
type statsData struct {
	Tokens struct {
		Input      int64 `json:"input"`
		Output     int64 `json:"output"`
		CacheRead  int64 `json:"cacheRead"`
		CacheWrite int64 `json:"cacheWrite"`
	} `json:"tokens"`
	Cost         float64 `json:"cost"`
	ContextUsage *struct {
		Percent *float64 `json:"percent"`
	} `json:"contextUsage"`
}

// assistantEnd extracts the stop reason from a message_end event when the
// completed message is an assistant message.
type assistantEnd struct {
	Message struct {
		Role       string `json:"role"`
		StopReason string `json:"stopReason"`
	} `json:"message"`
}

// imagePayload is pi's ImageContent wire shape for prompt images.
type imagePayload struct {
	Type     string `json:"type"`
	Data     string `json:"data"`
	MimeType string `json:"mimeType"`
}

// dialogMethods are the extension UI methods that block the agent until the
// client answers. Running headless, we always answer "cancelled" so an
// extension dialog can never wedge a session.
var dialogMethods = map[string]bool{
	"select":  true,
	"confirm": true,
	"input":   true,
	"editor":  true,
}

// scanLines reads strict JSONL: records are separated by '\n' only, with an
// optional trailing '\r' stripped. pi's docs call out that generic line
// readers splitting on U+2028/U+2029 corrupt the stream; bufio splitting on
// raw '\n' bytes is compliant.
func scanLines(r io.Reader, onLine func(line []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 {
			continue
		}
		if err := onLine(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

func decodeRaw(line []byte) (rawEvent, error) {
	var ev rawEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return rawEvent{}, fmt.Errorf("decode pi record: %w", err)
	}
	if ev.Type == "" {
		return rawEvent{}, fmt.Errorf("pi record missing type: %s", truncateForLog(line))
	}
	ev.raw = append(json.RawMessage(nil), line...)
	return ev, nil
}

func truncateForLog(b []byte) string {
	const max = 512
	s := string(b)
	if len(s) > max {
		s = s[:max] + "…"
	}
	return strings.ToValidUTF8(s, "�")
}
