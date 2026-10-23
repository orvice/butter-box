package pi

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// fakeHostPeer simulates the external extension socket, not relay internals.
// The real extension/provider path is tested independently by the smoke suite.
type fakeHostPeer struct {
	conn    net.Conn
	scanner *bufio.Scanner
}

func connectFakeHost(sessionID string) *fakeHostPeer {
	if os.Getenv("FAKE_PI_HOST") == "" {
		return nil
	}
	conn, err := net.Dial("unix", os.Getenv("BUTTERBOX_PI_HOST_SOCKET"))
	if err != nil {
		panic(err)
	}
	contract, err := os.ReadFile(os.Getenv("BUTTERBOX_PI_HOST_CONTRACT"))
	if err != nil {
		panic(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(contract))
	version := 1
	if os.Getenv("FAKE_PI_HOST") == "incompatible" {
		version = 99
	}
	h := &fakeHostPeer{conn: conn, scanner: bufio.NewScanner(conn)}
	h.scanner.Buffer(make([]byte, 4096), 1<<20)
	h.send(map[string]any{"type": "hello", "version": version, "hash": hash, "session_id": sessionID})
	return h
}

func (h *fakeHostPeer) send(message any) {
	data, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	_, _ = h.conn.Write(append(data, '\n'))
}

func (h *fakeHostPeer) read() map[string]any {
	if !h.scanner.Scan() {
		return nil
	}
	var msg map[string]any
	if err := json.Unmarshal(h.scanner.Bytes(), &msg); err != nil {
		panic(err)
	}
	return msg
}

func (h *fakeHostPeer) run(f *fakePi, prompt string) bool {
	h.send(map[string]any{"type": "begin", "id": "begin"})
	config := h.read()
	if !strings.HasPrefix(prompt, "host ") {
		return false
	}
	turnID, _ := config["turn_id"].(string)
	if turnID == "" {
		return false
	}
	if gate := os.Getenv("FAKE_PI_HOST_CALL_GATE"); gate != "" {
		if err := os.WriteFile(filepath.Join(gate, "captured"), nil, 0o600); err != nil {
			panic(err)
		}
		if err := waitForFakeFile(context.Background(), filepath.Join(gate, "release")); err != nil {
			panic(err)
		}
	}
	count := 1
	if strings.HasPrefix(prompt, "host calls ") {
		count, _ = strconv.Atoi(strings.TrimPrefix(prompt, "host calls "))
	}
	if prompt == "host disconnect" {
		_ = h.conn.Close()
		<-f.abortCh // model/tool work stays blocked until Box tears down the process
		return true
	}
	last := ""
	for i := range count {
		args := map[string]any{"messages": []any{}, "fallback": "summary"}
		if prompt == "host oversize" {
			args["fallback"] = strings.Repeat("x", 256*1024)
		}
		h.send(map[string]any{"type": "call", "turn_id": turnID, "call_id": fmt.Sprintf("call-%d", i+1), "tool": "render_ui", "arguments": args})
		if prompt == "host parallel" {
			h.send(map[string]any{"type": "call", "turn_id": turnID, "call_id": "parallel-call", "tool": "render_ui", "arguments": args})
			reply := h.read()
			last = fmt.Sprint(reply["error"])
		}
		reply := h.read()
		if reply == nil {
			return true
		}
		if result := reply["result"]; result != nil {
			data, _ := json.Marshal(result)
			last = string(data)
		} else {
			last = fmt.Sprint(reply["error"])
		}
	}
	f.appendEntry(map[string]any{"type": "message", "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []map[string]any{{"type": "text", "text": last}}}})
	f.emit(map[string]any{"type": "message_end", "message": map[string]any{"role": "assistant", "stopReason": "stop"}})
	return true
}
