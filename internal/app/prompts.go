package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const sandboxGuide = `You have access to ButterBox, an isolated Ubuntu sandbox exposed over MCP.

Available tools:
- ReadFile: read the full content of a file inside the sandbox root
- WriteFile: write full content to a file (set createDirs to create parent directories)
- Bash: run shell commands (bash) with optional cwd, timeoutSeconds, and env

Environment:
- Ubuntu 24.04, running as user "butterbox" with passwordless sudo (system packages: sudo apt-get install ...)
- Toolchains: Node.js 22 (npm), Python 3.12 (pip works out of the box), Go 1.26
- CLI tools: git, curl, wget, jq, ripgrep, unzip, zip, build-essential, vim, kubectl, aws, gcloud, rclone, gws (Google Workspace CLI)
- All file paths and working directories are confined to the sandbox root (%s); the Bash working directory defaults to it

Guidelines:
- Prefer running commands and inspecting real output over guessing
- Long-running commands should set timeoutSeconds explicitly (default is 30s)
- Command output is truncated at 1 MiB; write large results to files and read the parts you need`

func addPrompts(server *mcp.Server, cfg *Config) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "sandbox_task",
		Title:       "Run a task in the ButterBox sandbox",
		Description: "Instructions for completing a task inside the ButterBox sandbox using its ReadFile, WriteFile, and Bash tools",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "task",
				Description: "The task to carry out inside the sandbox",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		text := fmt.Sprintf(sandboxGuide, cfg.Root)
		if task := strings.TrimSpace(req.Params.Arguments["task"]); task != "" {
			text += "\n\nYour task:\n" + task
		}

		return &mcp.GetPromptResult{
			Description: "ButterBox sandbox task instructions",
			Messages: []*mcp.PromptMessage{
				{
					Role:    "user",
					Content: &mcp.TextContent{Text: text},
				},
			},
		}, nil
	})
}
