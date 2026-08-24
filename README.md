# ButterBox

ButterBox is a personal VM for agents, exposed over MCP and built with [github.com/modelcontextprotocol/go-sdk](https://github.com/modelcontextprotocol/go-sdk). It ships as an Ubuntu-based container image with common toolchains and CLI tools pre-installed — an always-on machine your agent can drive, isolated from where the agent itself runs, with a home directory that persists across sessions.

It exposes a `streamable HTTP` endpoint and provides 3 tools by default:

- `ReadFile`
- `WriteFile`
- `ExecCommand`

It also provides an MCP prompt:

- `exec_task` — instructions for completing a task on the VM; optional `task` argument appends the concrete task to carry out

It is designed to run well inside Docker, with environment variables for the listen address, workspace root, and bearer token authentication.

## Pre-installed Environment

The container image is based on `ubuntu:24.04`, runs as the non-root user `butterbox` (with passwordless `sudo`, so `sudo apt-get install` works), and ships with:

- **Node.js** 22 (NodeSource) + npm, global installs go to `~/.npm-global`
- **Python** 3.12 + pip + venv (`PIP_BREAK_SYSTEM_PACKAGES=1`, so `pip install` works out of the box)
- **Go** 1.26 toolchain (`GOPATH=~/go`, `~/go/bin` on `PATH`)
- Common CLI tools: `git`, `curl`, `wget`, `jq`, `ripgrep`, `unzip`, `zip`, `build-essential`, `openssh-client`, `vim`, `kubectl`
- Cloud tools: `aws` (AWS CLI v2), `gcloud` (Google Cloud CLI), `rclone`, `logcli` (Grafana Loki)
- Dev platform CLIs: `gh` (GitHub), `glab` (GitLab), `gog`, `td` (Todoist)
- Coding agent CLIs: `codex`, `opencode` (`/usr/bin/opencode`), `pi`, `pi-web` ([@agegr/pi-web](https://github.com/agegr/pi-web), browser UI for `pi`)
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
- `SANDBOX_ROOT`: workspace root for file access and command execution, default current directory
- `SANDBOX_SHELL`: shell used by the `ExecCommand` tool, default `bash`
- `MCP_STATELESS`: enable stateless streamable HTTP mode, default `false`
- `MCP_JSON_RESPONSE`: prefer `application/json` responses, default `false`
- `PI_WEB_ENABLED`: run [pi-web](https://github.com/agegr/pi-web) as a supervised child process and reverse-proxy it, default `false`
- `PI_WEB_PASSWORD`: required when `PI_WEB_ENABLED` is set; enables pi-web's built-in HTTP Basic Auth (username is always `pi`)
- `PI_WEB_PORT`: internal port pi-web listens on (loopback only), default `30141`
- `PI_API_ENABLED`: expose the ConnectRPC [pi session API](#pi-api) at `/butterbox.pi.v1.PiService/`, default `false`
- `PI_API_MAX_SESSIONS`: cap on concurrently active pi processes, default `8`
- `PI_API_IDLE_TIMEOUT`: stop a session process after this much inactivity (Go duration, session file is kept and re-attached on next use), default `30m`
- `PI_BIN`: pi executable, default `pi`
- `PI_SESSION_DIR`: override pi's session storage directory (passed as `--session-dir`)

## Pi Web

When `PI_WEB_ENABLED=true`, the server launches `pi-web` bound to `127.0.0.1` and reverse-proxies it at `/` on the main listen address (`/mcp` and `/healthz` keep precedence). The child process is restarted with backoff if it exits, and pi-web's own Basic Auth protects the UI:

```bash
docker run --rm -p 8080:8080 \
  -e MCP_AUTH_TOKEN=secret-token \
  -e PI_WEB_ENABLED=true \
  -e PI_WEB_PASSWORD=long-random-password \
  butter-box
```

Then open `http://127.0.0.1:8080/` and log in with username `pi` and the configured password. Do not expose it over plain HTTP to the internet — terminate TLS in front of it.

## Pi API

When `PI_API_ENABLED=true`, ButterBox exposes the [pi coding agent](https://github.com/earendil-works/pi) as an agent runtime over ConnectRPC. The service is defined in [`proto/butterbox/pi/v1/pi.proto`](proto/butterbox/pi/v1/pi.proto) and mounted at `/butterbox.pi.v1.PiService/`, protected by the same `MCP_AUTH_TOKEN` bearer auth as the MCP endpoint.

Each session maps to a supervised `pi --mode rpc` child process speaking pi's JSONL RPC protocol. Session IDs are pi's own session IDs and session data lives in pi's session directory, so idle sessions are stopped and transparently re-attached on next use — and every API-driven session shows up in pi-web when that is enabled too. Extension UI dialogs are auto-cancelled so a headless run can never wedge.

RPCs: `CreateSession`, `ListSessions`, `GetSession`, `GetAvailableModels` (models the session's pi process can use, with provider, modalities, limits, and USD-per-million-token costs), `SendMessage` (unary, returns after the run fully settles), `StreamMessage` (server stream of raw pi events, ends with `agent_settled`), `AbortSession`, `DeleteSession` (`purge: true` also removes the session file).

ConnectRPC speaks plain JSON over HTTP POST, so curl works:

```bash
curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/CreateSession \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-task","provider":"anthropic"}'

curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/SendMessage \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"<id>","message":"What files are here?"}'
```

pi uses its own model credentials (`~/.pi/agent` auth or provider environment variables such as `ANTHROPIC_API_KEY`); make sure they are present in the container environment or persisted home volume.

Regenerate the ConnectRPC code after editing the proto with `buf generate` (config in `buf.gen.yaml`, lint with `buf lint`).

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
