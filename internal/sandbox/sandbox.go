// Package sandbox validates user-supplied paths against a workspace root.
package sandbox

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Resolve resolves userPath — absolute, or relative to root — to a cleaned
// absolute path, rejecting paths that escape root.
func Resolve(root, userPath string) (string, error) {
	if strings.TrimSpace(userPath) == "" {
		return "", errors.New("path is required")
	}

	var candidate string
	if filepath.IsAbs(userPath) {
		candidate = filepath.Clean(userPath)
	} else {
		candidate = filepath.Join(root, userPath)
	}

	candidate = filepath.Clean(candidate)
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes workspace root %q", userPath, root)
	}
	return candidate, nil
}
