package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/orvice/butter-box/internal/sandbox"
)

const (
	defaultToolTimeout = 30 * time.Second
	maxCommandOutput   = 1 << 20 // 1 MiB
)

type readFileParams struct {
	Path string `json:"path" jsonschema:"Path to a file inside the workspace root"`
}

type readFileResult struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type writeFileParams struct {
	Path       string `json:"path" jsonschema:"Path to a file inside the workspace root"`
	Content    string `json:"content" jsonschema:"Full file content to write"`
	CreateDirs bool   `json:"createDirs,omitempty" jsonschema:"Create parent directories when true"`
}

type writeFileResult struct {
	Path    string `json:"path"`
	Bytes   int    `json:"bytes"`
	Created bool   `json:"created"`
}

type execParams struct {
	Command        string            `json:"command" jsonschema:"Shell command to execute on the ButterBox VM"`
	Cwd            string            `json:"cwd,omitempty" jsonschema:"Optional working directory relative to the workspace root"`
	TimeoutSeconds int               `json:"timeoutSeconds,omitempty" jsonschema:"Optional timeout in seconds, defaults to 30"`
	Env            map[string]string `json:"env,omitempty" jsonschema:"Optional environment variables for the command"`
}

type execResult struct {
	Cwd      string `json:"cwd"`
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

func NewMCPServer(cfg *Config) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "butter-box",
		Version: "0.1.0",
	}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ReadFile",
		Description: "Read the full content of a file inside the workspace root",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in readFileParams) (*mcp.CallToolResult, readFileResult, error) {
		target, err := resolveSandboxPath(cfg.Root, in.Path)
		if err != nil {
			return nil, readFileResult{}, err
		}

		data, err := os.ReadFile(target)
		if err != nil {
			return nil, readFileResult{}, fmt.Errorf("read file: %w", err)
		}

		out := readFileResult{
			Path:    target,
			Content: string(data),
		}
		return textResult(out.Content), out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "WriteFile",
		Description: "Write full content to a file inside the workspace root",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in writeFileParams) (*mcp.CallToolResult, writeFileResult, error) {
		target, err := resolveSandboxPath(cfg.Root, in.Path)
		if err != nil {
			return nil, writeFileResult{}, err
		}

		if in.CreateDirs {
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return nil, writeFileResult{}, fmt.Errorf("create parent dirs: %w", err)
			}
		}

		_, statErr := os.Stat(target)
		created := errors.Is(statErr, os.ErrNotExist)

		if err := os.WriteFile(target, []byte(in.Content), 0o644); err != nil {
			return nil, writeFileResult{}, fmt.Errorf("write file: %w", err)
		}

		out := writeFileResult{
			Path:    target,
			Bytes:   len(in.Content),
			Created: created,
		}
		return textResult(fmt.Sprintf("wrote %d bytes to %s", out.Bytes, out.Path)), out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ExecCommand",
		Description: "Execute a shell command inside the workspace root",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in execParams) (*mcp.CallToolResult, execResult, error) {
		if strings.TrimSpace(in.Command) == "" {
			return nil, execResult{}, errors.New("command is required")
		}

		cwd := cfg.Root
		var err error
		if strings.TrimSpace(in.Cwd) != "" {
			cwd, err = resolveSandboxPath(cfg.Root, in.Cwd)
			if err != nil {
				return nil, execResult{}, err
			}
		}

		timeout := defaultToolTimeout
		if in.TimeoutSeconds > 0 {
			timeout = time.Duration(in.TimeoutSeconds) * time.Second
		}

		runCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		cmd := exec.CommandContext(runCtx, cfg.Shell, "-lc", in.Command)
		cmd.Dir = cwd
		cmd.Env = append(os.Environ(), envMapToList(in.Env)...)

		stdout, stderr, exitCode, err := runCommand(cmd)
		if err != nil && runCtx.Err() == context.DeadlineExceeded {
			err = fmt.Errorf("command timed out after %s", timeout)
		}
		if err != nil && exitCode == 0 {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				return nil, execResult{}, err
			}
		}

		out := execResult{
			Cwd:      cwd,
			ExitCode: exitCode,
			Stdout:   stdout,
			Stderr:   stderr,
		}
		resultText := fmt.Sprintf("exitCode=%d\nstdout:\n%s\nstderr:\n%s", out.ExitCode, emptyFallback(out.Stdout), emptyFallback(out.Stderr))
		if err != nil {
			resultText += "\nerror: " + err.Error()
		}
		return textResult(resultText), out, nil
	})

	addPrompts(server, cfg)

	return server
}

func resolveSandboxPath(root, userPath string) (string, error) {
	return sandbox.Resolve(root, userPath)
}

func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}
}

func envMapToList(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out
}

func emptyFallback(s string) string {
	if s == "" {
		return "(empty)"
	}
	return s
}
