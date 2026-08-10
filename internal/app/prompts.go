package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const vmGuide = `You have access to ButterBox, your personal Ubuntu VM, exposed over MCP.

Available tools:
- ReadFile: read the full content of a file inside the workspace root
- WriteFile: write full content to a file (set createDirs to create parent directories)
- ExecCommand: run shell commands (bash) with optional cwd, timeoutSeconds, and env

Environment:
- Ubuntu 24.04, running as user "butterbox" with passwordless sudo (system packages: sudo apt-get install ...)
- Toolchains: Node.js 22 (npm), Python 3.12 (pip works out of the box), Go 1.26
- CLI tools: git, curl, wget, jq, ripgrep, unzip, zip, build-essential, vim, kubectl, aws, gcloud, rclone, logcli, gh, glab, gog, td, gws (Google Workspace CLI)
- Your home directory persists across sessions: installed packages, CLI auth state, and shell history stick around
- All file paths and working directories are confined to the workspace root (%s); the ExecCommand working directory defaults to it

Guidelines:
- Prefer running commands and inspecting real output over guessing
- Long-running commands should set timeoutSeconds explicitly (default is 30s)
- Command output is truncated at 1 MiB; write large results to files and read the parts you need`

func addPrompts(server *mcp.Server, cfg *Config) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "exec_task",
		Title:       "Run a task on your ButterBox VM",
		Description: "Instructions for completing a task on your personal ButterBox VM using its ReadFile, WriteFile, and ExecCommand tools",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "task",
				Description: "The task to carry out on the VM",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		text := fmt.Sprintf(vmGuide, cfg.Root)
		if task := strings.TrimSpace(req.Params.Arguments["task"]); task != "" {
			text += "\n\nYour task:\n" + task
		}

		return &mcp.GetPromptResult{
			Description: "ButterBox task instructions",
			Messages: []*mcp.PromptMessage{
				{
					Role:    "user",
					Content: &mcp.TextContent{Text: text},
				},
			},
		}, nil
	})
}
