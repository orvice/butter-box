package pi

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// findSessionCwd locates session id's JSONL file under root and returns the
// working directory recorded in its session header ("" when not found). pi
// stores sessions either flat in a custom --session-dir or grouped in
// per-cwd subdirectories of the default directory, so both root and its
// immediate subdirectories are searched. File names end in "_<id>.jsonl".
func findSessionCwd(root, id string) string {
	if root == "" || id == "" {
		return ""
	}
	dirs := []string{root}
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				dirs = append(dirs, filepath.Join(root, e.Name()))
			}
		}
	}
	suffix := id + ".jsonl"
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
				continue
			}
			if cwd := readSessionHeaderCwd(filepath.Join(dir, e.Name()), id); cwd != "" {
				return cwd
			}
		}
	}
	return ""
}

// readSessionHeaderCwd reads the session header (first JSONL record) and
// returns its cwd when the header belongs to session id.
func readSessionHeaderCwd(path, id string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	if !sc.Scan() {
		return ""
	}
	var header struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Cwd  string `json:"cwd"`
	}
	if err := json.Unmarshal(sc.Bytes(), &header); err != nil {
		return ""
	}
	if header.Type != "session" || header.ID != id {
		return ""
	}
	return header.Cwd
}
