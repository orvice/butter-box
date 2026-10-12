package pi

import (
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Record only the external Pi protocol, in temporary files with fake data.
func recordFakeCommand(cmd map[string]any) {
	path := os.Getenv("FAKE_PI_COMMAND_LOG")
	if path == "" {
		return
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		panic(err)
	}
}

func commandLogSetup(t *testing.T) func() []string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "commands.jsonl")
	t.Setenv("FAKE_PI_COMMAND_LOG", path)
	return func() []string {
		t.Helper()
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		var commands []string
		for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var cmd struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(line), &cmd); err != nil {
				t.Fatal(err)
			}
			commands = append(commands, cmd.Type)
		}
		return commands
	}
}
