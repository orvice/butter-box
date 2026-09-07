package app

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfig_RequiresAuthToken(t *testing.T) {
	t.Setenv("MCP_AUTH_TOKEN", "")
	_, err := LoadConfig()
	if err == nil || !strings.Contains(err.Error(), "MCP_AUTH_TOKEN") {
		t.Fatalf("expected MCP_AUTH_TOKEN is required error, got %v", err)
	}
}

func TestLoadConfig_AcceptsAuthToken(t *testing.T) {
	t.Setenv("MCP_AUTH_TOKEN", "  secret-token  ")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Token != "secret-token" {
		t.Errorf("Token = %q, want %q", cfg.Token, "secret-token")
	}
}

func TestLoadConfig_CursorAPI(t *testing.T) {
	t.Setenv("MCP_AUTH_TOKEN", "box-token")
	t.Setenv("CURSOR_API_ENABLED", "true")
	t.Setenv("CURSOR_API_KEY", "cursor-key")
	t.Setenv("CURSOR_AUTH_TOKEN", "cursor-token")
	t.Setenv("CURSOR_SDK_BRIDGE_BIN", "/usr/local/bin/cursor-sdk-bridge")
	t.Setenv("CURSOR_MAX_SESSIONS", "3")
	t.Setenv("CURSOR_API_IDLE_TIMEOUT", "45s")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Cursor.Enabled || cfg.Cursor.APIKey != "cursor-key" {
		t.Fatalf("Cursor config = %+v", cfg.Cursor)
	}
	if cfg.Cursor.AuthToken != "cursor-token" || cfg.Cursor.Bin != "/usr/local/bin/cursor-sdk-bridge" {
		t.Fatalf("Cursor auth/bin = %+v", cfg.Cursor)
	}
	if cfg.Cursor.MaxSessions != 3 || cfg.Cursor.IdleTimeout != 45*time.Second {
		t.Fatalf("Cursor limits = %+v", cfg.Cursor)
	}
}

func TestLoadConfig_CursorMaxSessionsAlignsWithPi(t *testing.T) {
	t.Setenv("MCP_AUTH_TOKEN", "box-token")
	t.Setenv("PI_API_ENABLED", "true")
	t.Setenv("PI_API_MAX_SESSIONS", "5")
	t.Setenv("CURSOR_API_ENABLED", "true")
	t.Setenv("CURSOR_MAX_SESSIONS", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Cursor.MaxSessions != 5 {
		t.Fatalf("Cursor MaxSessions = %d, want 5", cfg.Cursor.MaxSessions)
	}
}
