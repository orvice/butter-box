package pi

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// sessionSearchDirs returns the directories that may hold session JSONL
// files: pi stores sessions either flat in a custom --session-dir or grouped
// in per-cwd subdirectories of the default directory, so root and its
// immediate subdirectories are both searched.
func sessionSearchDirs(root string) []string {
	dirs := []string{root}
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				dirs = append(dirs, filepath.Join(root, e.Name()))
			}
		}
	}
	return dirs
}

// findSessionCwd locates session id's JSONL file under root and returns the
// working directory recorded in its session header ("" when not found).
// File names end in "_<id>.jsonl".
func findSessionCwd(root, id string) string {
	if root == "" || id == "" {
		return ""
	}
	suffix := id + ".jsonl"
	for _, dir := range sessionSearchDirs(root) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
				continue
			}
			if hdr, ok := readSessionHeader(filepath.Join(dir, e.Name())); ok && hdr.ID == id && hdr.Cwd != "" {
				return hdr.Cwd
			}
		}
	}
	return ""
}

// fileSession is one session discovered in pi's session directory.
type fileSession struct {
	ID      string
	Cwd     string
	File    string
	ModTime time.Time
}

// listFileSessions scans root for session JSONL files and returns one record
// per valid session header, keeping the newest file when an id appears more
// than once. A missing or unreadable root yields an empty list.
func listFileSessions(root string) []fileSession {
	if root == "" {
		return nil
	}
	byID := map[string]int{}
	var out []fileSession
	for _, dir := range sessionSearchDirs(root) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			hdr, ok := readSessionHeader(path)
			if !ok || hdr.ID == "" {
				continue
			}
			fi, err := e.Info()
			if err != nil {
				continue
			}
			fs := fileSession{ID: hdr.ID, Cwd: hdr.Cwd, File: path, ModTime: fi.ModTime()}
			if i, dup := byID[hdr.ID]; dup {
				if fs.ModTime.After(out[i].ModTime) {
					out[i] = fs
				}
				continue
			}
			byID[hdr.ID] = len(out)
			out = append(out, fs)
		}
	}
	return out
}

// sessionHeader is the first JSONL record of a pi session file.
type sessionHeader struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Cwd  string `json:"cwd"`
}

// readSessionHeader reads a file's session header; ok is false when the
// first record is not a session header.
func readSessionHeader(path string) (sessionHeader, bool) {
	f, err := os.Open(path)
	if err != nil {
		return sessionHeader{}, false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	if !sc.Scan() {
		return sessionHeader{}, false
	}
	var header sessionHeader
	if err := json.Unmarshal(sc.Bytes(), &header); err != nil {
		return sessionHeader{}, false
	}
	if header.Type != "session" {
		return sessionHeader{}, false
	}
	return header, true
}
