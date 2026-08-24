package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
}

type PiWebConfig struct {
	Enabled  bool
	Port     int
	Password string
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

	return &Config{
		Addr:         getenvDefault("MCP_ADDR", defaultAddr),
		Token:        os.Getenv("MCP_AUTH_TOKEN"),
		Root:         filepath.Clean(absRoot),
		Shell:        getenvDefault("SANDBOX_SHELL", "bash"),
		HTTPPath:     httpPath,
		Stateless:    envBool("MCP_STATELESS", false),
		JSONResponse: envBool("MCP_JSON_RESPONSE", false),
		PiWeb:        piWeb,
	}, nil
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
