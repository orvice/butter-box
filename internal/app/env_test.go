package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEnvironmentFile_LoadsUnsetValues(t *testing.T) {
	const (
		plainKey  = "BUTTERBOX_TEST_DOTENV_PLAIN"
		quotedKey = "BUTTERBOX_TEST_DOTENV_QUOTED"
	)
	unsetEnvironmentVariable(t, plainKey)
	unsetEnvironmentVariable(t, quotedKey)

	path := filepath.Join(t.TempDir(), ".env")
	contents := plainKey + "=from-file\n" + quotedKey + "=\"hello world\"\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write environment file: %v", err)
	}

	loaded, err := loadEnvironmentFile(path)
	if err != nil {
		t.Fatalf("loadEnvironmentFile: %v", err)
	}
	if !loaded {
		t.Fatal("loadEnvironmentFile reported that an existing file was not loaded")
	}
	if got := os.Getenv(plainKey); got != "from-file" {
		t.Errorf("%s = %q, want %q", plainKey, got, "from-file")
	}
	if got := os.Getenv(quotedKey); got != "hello world" {
		t.Errorf("%s = %q, want %q", quotedKey, got, "hello world")
	}
}

func TestLoadEnvironmentFile_PreservesExistingValues(t *testing.T) {
	const key = "BUTTERBOX_TEST_DOTENV_PRECEDENCE"
	t.Setenv(key, "from-process")

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(key+"=from-file\n"), 0o600); err != nil {
		t.Fatalf("write environment file: %v", err)
	}

	loaded, err := loadEnvironmentFile(path)
	if err != nil {
		t.Fatalf("loadEnvironmentFile: %v", err)
	}
	if !loaded {
		t.Fatal("loadEnvironmentFile reported that an existing file was not loaded")
	}
	if got := os.Getenv(key); got != "from-process" {
		t.Errorf("%s = %q, want existing value %q", key, got, "from-process")
	}
}

func TestLoadEnvironmentFile_IgnoresMissingFile(t *testing.T) {
	loaded, err := loadEnvironmentFile(filepath.Join(t.TempDir(), ".env"))
	if err != nil {
		t.Fatalf("loadEnvironmentFile: %v", err)
	}
	if loaded {
		t.Fatal("loadEnvironmentFile reported loading a missing file")
	}
}

func TestLoadEnvironmentFile_RejectsMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("lol$wut\n"), 0o600); err != nil {
		t.Fatalf("write environment file: %v", err)
	}

	loaded, err := loadEnvironmentFile(path)
	if err == nil {
		t.Fatal("loadEnvironmentFile succeeded for malformed input")
	}
	if loaded {
		t.Fatal("loadEnvironmentFile reported loading malformed input")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not identify environment file %q", err, path)
	}
}

func unsetEnvironmentVariable(t *testing.T, key string) {
	t.Helper()
	value, existed := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		var err error
		if existed {
			err = os.Setenv(key, value)
		} else {
			err = os.Unsetenv(key)
		}
		if err != nil {
			t.Errorf("restore %s: %v", key, err)
		}
	})
}
