package app

import (
	"strings"
	"testing"
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
