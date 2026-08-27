package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddr      = ":8080"
	defaultPiWebPort = 30141
)

type Config struct {
	Addr         string
	Token        string
	Root         string
	Shell        string
	HTTPPath     string
	Stateless    bool
	JSONResponse bool
	PiWeb        PiWebConfig
	PiAPI        PiAPIConfig
	Cursor       CursorAPIConfig
}

type PiWebConfig struct {
	Enabled  bool
	Port     int
	Password string
}

// PiAPIConfig configures the ConnectRPC pi session API. Requests authenticate
// with the same bearer token as the MCP endpoint.
type PiAPIConfig struct {
	Enabled     bool
	Bin         string
	MaxSessions int
	IdleTimeout time.Duration
	SessionDir  string
}

// CursorAPIConfig configures the Cursor SDK Bridge session API. The Cursor
// API key is passed to the bridge and is never returned by the box service.
type CursorAPIConfig struct {
	Enabled     bool
	Bin         string
	APIKey      string
	AuthToken   string
	MaxSessions int
	IdleTimeout time.Duration
}

func LoadConfig() (*Config, error) {
	root := getenvDefault("SANDBOX_ROOT", ".")
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve SANDBOX_ROOT: %w", err)
	}

	httpPath := getenvDefault("MCP_HTTP_PATH", "/mcp")
	if httpPath == "" || !strings.HasPrefix(httpPath, "/") {
		return nil, errors.New("MCP_HTTP_PATH must start with '/'")
	}

	piWeb, err := loadPiWebConfig()
	if err != nil {
		return nil, err
	}

	piAPI, err := loadPiAPIConfig()
	if err != nil {
		return nil, err
	}

	cursorAPI, err := loadCursorAPIConfig(piAPI)
	if err != nil {
		return nil, err
	}

	token := strings.TrimSpace(os.Getenv("MCP_AUTH_TOKEN"))
	if token == "" {
		return nil, errors.New("MCP_AUTH_TOKEN is required: refusing to run with an unauthenticated MCP endpoint and Pi API")
	}

	return &Config{
		Addr:         getenvDefault("MCP_ADDR", defaultAddr),
		Token:        token,
		Root:         filepath.Clean(absRoot),
		Shell:        getenvDefault("SANDBOX_SHELL", "bash"),
		HTTPPath:     httpPath,
		Stateless:    envBool("MCP_STATELESS", false),
		JSONResponse: envBool("MCP_JSON_RESPONSE", false),
		PiWeb:        piWeb,
		PiAPI:        piAPI,
		Cursor:       cursorAPI,
	}, nil
}

func loadPiAPIConfig() (PiAPIConfig, error) {
	cfg := PiAPIConfig{
		Enabled:    envBool("PI_API_ENABLED", false),
		Bin:        getenvDefault("PI_BIN", "pi"),
		SessionDir: strings.TrimSpace(os.Getenv("PI_SESSION_DIR")),
	}
	if !cfg.Enabled {
		return cfg, nil
	}

	maxSessions := getenvDefault("PI_API_MAX_SESSIONS", "8")
	parsed, err := strconv.Atoi(maxSessions)
	if err != nil || parsed <= 0 {
		return cfg, fmt.Errorf("PI_API_MAX_SESSIONS must be a positive integer, got %q", maxSessions)
	}
	cfg.MaxSessions = parsed

	idle := getenvDefault("PI_API_IDLE_TIMEOUT", "30m")
	timeout, err := time.ParseDuration(idle)
	if err != nil || timeout <= 0 {
		return cfg, fmt.Errorf("PI_API_IDLE_TIMEOUT must be a positive duration, got %q", idle)
	}
	cfg.IdleTimeout = timeout
	return cfg, nil
}

func loadCursorAPIConfig(piCfg PiAPIConfig) (CursorAPIConfig, error) {
	cfg := CursorAPIConfig{
		Enabled:   envBool("CURSOR_API_ENABLED", false),
		Bin:       getenvDefault("CURSOR_SDK_BRIDGE_BIN", "cursor-sdk-bridge"),
		APIKey:    strings.TrimSpace(os.Getenv("CURSOR_API_KEY")),
		AuthToken: strings.TrimSpace(os.Getenv("CURSOR_AUTH_TOKEN")),
	}
	if !cfg.Enabled {
		return cfg, nil
	}

	maxText := strings.TrimSpace(os.Getenv("CURSOR_MAX_SESSIONS"))
	if maxText == "" && piCfg.MaxSessions > 0 {
		cfg.MaxSessions = piCfg.MaxSessions
	} else {
		if maxText == "" {
			maxText = strings.TrimSpace(os.Getenv("PI_API_MAX_SESSIONS"))
		}
		if maxText == "" {
			maxText = "8"
		}
		parsed, err := strconv.Atoi(maxText)
		if err != nil || parsed <= 0 {
			return cfg, fmt.Errorf("CURSOR_MAX_SESSIONS must be a positive integer, got %q", maxText)
		}
		cfg.MaxSessions = parsed
	}

	idleText := strings.TrimSpace(os.Getenv("CURSOR_API_IDLE_TIMEOUT"))
	if idleText == "" {
		idleText = "30m"
	}
	timeout, err := time.ParseDuration(idleText)
	if err != nil || timeout <= 0 {
		return cfg, fmt.Errorf("CURSOR_API_IDLE_TIMEOUT must be a positive duration, got %q", idleText)
	}
	cfg.IdleTimeout = timeout
	return cfg, nil
}

func loadPiWebConfig() (PiWebConfig, error) {
	cfg := PiWebConfig{
		Enabled:  envBool("PI_WEB_ENABLED", false),
		Password: os.Getenv("PI_WEB_PASSWORD"),
	}
	if !cfg.Enabled {
		return cfg, nil
	}

	// pi-web enforces HTTP Basic Auth itself (username "pi") when
	// PI_WEB_PASSWORD is set; refuse to proxy an unauthenticated instance.
	if strings.TrimSpace(cfg.Password) == "" {
		return cfg, errors.New("PI_WEB_PASSWORD is required when PI_WEB_ENABLED is set")
	}

	port, err := envInt("PI_WEB_PORT", defaultPiWebPort)
	if err != nil {
		return cfg, err
	}
	cfg.Port = port
	return cfg, nil
}

func getenvDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || parsed > 65535 {
		return 0, fmt.Errorf("%s must be a valid port number, got %q", key, value)
	}
	return parsed, nil
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}
