package pi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// mkdirs creates each path under root, joining with the OS separator.
func mkdirs(t *testing.T, root string, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if err := os.MkdirAll(filepath.Join(root, p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// dirNames extracts the listed directory names, in order.
func dirNames(listing DirListing) []string {
	names := make([]string, len(listing.Directories))
	for i, d := range listing.Directories {
		names[i] = d.Name
	}
	return names
}

func TestListDirectories(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "alpha", "beta/nested", ".hidden")
	if err := os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SandboxRoot: root})

	// Empty path lists the sandbox root: directories only, hidden skipped,
	// sorted by name.
	listing, err := m.ListDirectories("", false)
	if err != nil {
		t.Fatalf("ListDirectories(root): %v", err)
	}
	sameDir(t, listing.Path, root)
	if got := dirNames(listing); len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Fatalf("root listing = %v, want [alpha beta]", got)
	}
	if listing.Truncated {
		t.Fatalf("root listing truncated, want false")
	}
	sameDir(t, listing.Directories[0].Path, filepath.Join(root, "alpha"))

	// include_hidden adds dot-directories, still no files.
	listing, err = m.ListDirectories("", true)
	if err != nil {
		t.Fatalf("ListDirectories(include hidden): %v", err)
	}
	if got := dirNames(listing); len(got) != 3 || got[0] != ".hidden" {
		t.Fatalf("hidden listing = %v, want [.hidden alpha beta]", got)
	}

	// One level down, addressed relatively and absolutely.
	for _, path := range []string{"beta", filepath.Join(root, "beta")} {
		listing, err := m.ListDirectories(path, false)
		if err != nil {
			t.Fatalf("ListDirectories(%q): %v", path, err)
		}
		if got := dirNames(listing); len(got) != 1 || got[0] != "nested" {
			t.Fatalf("ListDirectories(%q) = %v, want [nested]", path, got)
		}
	}

	// A leaf directory lists as empty, not as an error.
	listing, err = m.ListDirectories("alpha", false)
	if err != nil {
		t.Fatalf("ListDirectories(leaf): %v", err)
	}
	if len(listing.Directories) != 0 {
		t.Fatalf("leaf listing = %v, want empty", dirNames(listing))
	}
}

func TestListDirectoriesRejects(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "alpha")
	if err := os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A symlink to a directory outside the root must not become a way out.
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape-link")); err != nil {
		t.Fatal(err)
	}
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SandboxRoot: root})

	for _, path := range []string{"../escape", "/etc", "does-not-exist", "afile"} {
		if _, err := m.ListDirectories(path, false); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("ListDirectories(%q) = %v, want ErrInvalidPath", path, err)
		}
	}

	// A symlinked directory is not listed, so the picker never offers one.
	listing, err := m.ListDirectories("", false)
	if err != nil {
		t.Fatalf("ListDirectories(root): %v", err)
	}
	if got := dirNames(listing); len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("root listing = %v, want [alpha]", got)
	}
}

func TestListDirectoriesWithoutSandboxRoot(t *testing.T) {
	m := newFakeManagerCfg(t, Config{MaxSessions: 4})
	if _, err := m.ListDirectories("", false); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("ListDirectories without root = %v, want ErrInvalidPath", err)
	}
}

func TestListDirectoriesTruncates(t *testing.T) {
	root := t.TempDir()
	for i := 0; i <= maxDirectoryEntries; i++ {
		mkdirs(t, root, fmt.Sprintf("dir-%04d", i))
	}
	m := newFakeManagerCfg(t, Config{MaxSessions: 4, SandboxRoot: root})

	listing, err := m.ListDirectories("", false)
	if err != nil {
		t.Fatalf("ListDirectories: %v", err)
	}
	if len(listing.Directories) != maxDirectoryEntries {
		t.Fatalf("listed %d directories, want %d", len(listing.Directories), maxDirectoryEntries)
	}
	if !listing.Truncated {
		t.Fatalf("listing over the cap not marked truncated")
	}
}
