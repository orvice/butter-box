package pi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/orvice/butter-box/internal/sandbox"
)

// maxDirectoryEntries caps one ListDirectories answer; a listing that hits the
// cap is reported as truncated rather than silently cut short.
const maxDirectoryEntries = 500

// DirEntry is one subdirectory of a listed directory.
type DirEntry struct {
	Name string
	// Path is absolute, usable as a session cwd or a further listing path.
	Path string
}

// DirListing is one level of the sandbox directory tree.
type DirListing struct {
	// Path is the absolute path of the listed directory.
	Path        string
	Directories []DirEntry
	// Truncated is true when the listing hit maxDirectoryEntries.
	Truncated bool
}

// ListDirectories lists the immediate subdirectories of userPath (empty means
// the sandbox root). It is read-only and non-recursive — one level per call.
// Symlinked directories are not reported, so a picker walking these listings
// never offers a path out of the sandbox root.
func (m *Manager) ListDirectories(userPath string, includeHidden bool) (DirListing, error) {
	dir, err := m.resolveDir(userPath)
	if err != nil {
		return DirListing{}, fmt.Errorf("%w: %v", ErrInvalidPath, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return DirListing{}, fmt.Errorf("read directory: %w", err)
	}

	// os.ReadDir sorts by name, so the listing is already picker-ready.
	listing := DirListing{Path: dir}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !includeHidden && strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if len(listing.Directories) == maxDirectoryEntries {
			listing.Truncated = true
			break
		}
		listing.Directories = append(listing.Directories, DirEntry{
			Name: entry.Name(),
			Path: filepath.Join(dir, entry.Name()),
		})
	}
	return listing, nil
}

// resolveDir resolves a caller-supplied directory against the sandbox root
// using the same rules as the MCP tools: empty means the root itself, escapes
// are rejected, and the result must be an existing directory.
func (m *Manager) resolveDir(userPath string) (string, error) {
	if m.cfg.SandboxRoot == "" {
		return "", fmt.Errorf("no sandbox root configured")
	}

	resolved := m.cfg.SandboxRoot
	if strings.TrimSpace(userPath) != "" {
		var err error
		resolved, err = sandbox.Resolve(m.cfg.SandboxRoot, userPath)
		if err != nil {
			return "", err
		}
	}

	fi, err := os.Stat(resolved)
	if err != nil || !fi.IsDir() {
		return "", fmt.Errorf("%q is not an existing directory", resolved)
	}
	return resolved, nil
}
