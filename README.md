# ButterBox

ButterBox is a sandbox MCP server for agents, built with [github.com/modelcontextprotocol/go-sdk](https://github.com/modelcontextprotocol/go-sdk). It ships as an Ubuntu-based container image with common toolchains pre-installed, so agents get an isolated, ready-to-use execution environment separate from where the agent itself runs.

It exposes a `streamable HTTP` endpoint and provides 3 tools by default:

- `ReadFile`
- `WriteFile`
- `ExecCommand`

It also provides an MCP prompt:

- `exec_task` — instructions for completing a task inside the sandbox; optional `task` argument appends the concrete task to carry out

It is designed to run well inside Docker, with environment variables for the listen address, sandbox root, and bearer token authentication.

## Pre-installed Environment

The container image is based on `ubuntu:24.04`, runs as the non-root user `butterbox` (with passwordless `sudo`, so `sudo apt-get install` works), and ships with:

- **Node.js** 22 (NodeSource) + npm, global installs go to `~/.npm-global`
- **Python** 3.12 + pip + venv (`PIP_BREAK_SYSTEM_PACKAGES=1`, so `pip install` works out of the box)
- **Go** 1.26 toolchain (`GOPATH=~/go`, `~/go/bin` on `PATH`)
- Common CLI tools: `git`, `curl`, `wget`, `jq`, `ripgrep`, `unzip`, `zip`, `build-essential`, `openssh-client`, `vim`, `kubectl`
- Cloud tools: `aws` (AWS CLI v2), `gcloud` (Google Cloud CLI), `rclone`, `logcli` (Grafana Loki)
- Dev platform CLIs: `gh` (GitHub), `glab` (GitLab), `gog`, `td` (Todoist)
- [`gws`](https://github.com/googleworkspace/cli) — Google Workspace CLI (Drive, Gmail, Calendar, Sheets, and more)

## Local Run

```bash
go run .
```

Default endpoints:

- MCP endpoint: `http://127.0.0.1:8080/mcp`
- health: `http://127.0.0.1:8080/healthz`

Example:

```bash
MCP_ADDR=:8080 \
SANDBOX_ROOT=/workspace \
MCP_AUTH_TOKEN=secret-token \
go run .
```

## Docker Run

```bash
docker build -t butter-box .

docker run --rm -p 8080:8080 \
  -e MCP_AUTH_TOKEN=secret-token \
  -e SANDBOX_ROOT=/workspace \
  -v "$PWD:/workspace" \
  butter-box
```

## Docker Compose

Example `compose.yaml`:

```yaml
services:
  butter-box:
    image: ghcr.io/orvice/butter-box:main
    ports:
      - "8080:8080"
    environment:
      MCP_ADDR: ":8080"
      MCP_HTTP_PATH: "/mcp"
      MCP_AUTH_TOKEN: "secret-token"
      SANDBOX_ROOT: "/workspace"
      SANDBOX_SHELL: "/bin/bash"
    volumes:
      - /tmp/sandbox-workspace:/workspace
      # Persist the butterbox user's home dir: CLI auth state (e.g. gws
      # OAuth tokens), npm/pip/go user installs, shell history, etc.
      - butterbox-home:/home/butterbox
    restart: unless-stopped

volumes:
  butterbox-home:
```

Start it with:

```bash
docker compose up -d
```

## Environment Variables

- `MCP_ADDR`: HTTP listen address, default `:8080`
- `MCP_HTTP_PATH`: MCP HTTP path, default `/mcp`
- `MCP_AUTH_TOKEN`: when set, requires `Authorization: Bearer <token>`
- `SANDBOX_ROOT`: root directory for file access and command execution, default current directory
- `SANDBOX_SHELL`: shell used by the `ExecCommand` tool, default `bash`
- `MCP_STATELESS`: enable stateless streamable HTTP mode, default `false`
- `MCP_JSON_RESPONSE`: prefer `application/json` responses, default `false`

## MCP Server JSON Example

Example client configuration for a streamable HTTP MCP server with bearer auth:

```json
{
  "mcpServers": {
    "butter-box": {
      "type": "http",
      "url": "http://127.0.0.1:8080/mcp",
      "headers": {
        "Authorization": "Bearer secret-token"
      }
    }
  }
}
```

If your MCP client uses a different schema, keep the same core values:

- endpoint: `http://127.0.0.1:8080/mcp`
- auth header: `Authorization: Bearer <token>`

## Tools

### `ReadFile`

Request payload:

```json
{
  "path": "relative/or/absolute/path"
}
```

### `WriteFile`

Request payload:

```json
{
  "path": "tmp/hello.txt",
  "content": "hello world",
  "createDirs": true
}
```

### `ExecCommand`

Request payload:

```json
{
  "command": "pwd && ls -la",
  "cwd": ".",
  "timeoutSeconds": 30,
  "env": {
    "FOO": "bar"
  }
}
```

The result includes:

- `cwd`
- `exitCode`
- `stdout`
- `stderr`

## Notes

- All file paths are constrained to `SANDBOX_ROOT` to prevent path escape.
- The `ExecCommand` tool working directory is also constrained to `SANDBOX_ROOT`.
- Command output is truncated to avoid returning excessively large responses.
