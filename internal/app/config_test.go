package app

import (
	"path/filepath"
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

func setLinearEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MCP_AUTH_TOKEN", "box-token")
	t.Setenv("SANDBOX_ROOT", t.TempDir())
	t.Setenv("LINEAR_ENABLED", "true")
	t.Setenv("LINEAR_CLIENT_ID", "client-id")
	t.Setenv("LINEAR_CLIENT_SECRET", "client-secret")
	t.Setenv("LINEAR_WEBHOOK_SECRET", "webhook-secret")
	t.Setenv("LINEAR_INSTALL_SECRET", "install-secret-0123456789")
	t.Setenv("LINEAR_BASE_URL", "https://box.example.com/")
}

func TestLoadConfig_Linear(t *testing.T) {
	setLinearEnv(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PI_API_ENABLED", "")
	t.Setenv("PI_API_MAX_SESSIONS", "3")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	l := cfg.Linear
	if !l.Enabled || l.BaseURL != "https://box.example.com" || l.Cwd != "." || l.RunTimeout != 30*time.Minute {
		t.Fatalf("Linear config = %+v", l)
	}
	if l.StateDir != filepath.Join(home, ".butterbox", "linear") {
		t.Fatalf("StateDir = %q", l.StateDir)
	}
	// The integration shares the pi session manager, so its settings load
	// even with the Pi API off.
	if cfg.PiAPI.Enabled || cfg.PiAPI.MaxSessions != 3 {
		t.Fatalf("PiAPI = %+v", cfg.PiAPI)
	}
}

func TestLoadConfig_LinearRejects(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"missing secrets", map[string]string{"LINEAR_CLIENT_SECRET": "", "LINEAR_WEBHOOK_SECRET": ""}, "LINEAR_CLIENT_SECRET, LINEAR_WEBHOOK_SECRET"},
		{"short install secret", map[string]string{"LINEAR_INSTALL_SECRET": "short"}, "at least 16"},
		{"relative base url", map[string]string{"LINEAR_BASE_URL": "box.example.com"}, "LINEAR_BASE_URL"},
		{"cwd outside sandbox", map[string]string{"LINEAR_PI_CWD": "../elsewhere"}, "LINEAR_PI_CWD"},
		{"bad timeout", map[string]string{"LINEAR_RUN_TIMEOUT": "soon"}, "LINEAR_RUN_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLinearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}
