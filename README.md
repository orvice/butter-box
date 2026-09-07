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
MCP_ADDR=:8080 \
SANDBOX_ROOT=/workspace \
MCP_AUTH_TOKEN=secret-token \
go run .
```

`MCP_AUTH_TOKEN` is required — the server refuses to start without it
(see [Environment Variables](#environment-variables)).

Default endpoints:

- MCP endpoint: `http://127.0.0.1:8080/mcp`
- health: `http://127.0.0.1:8080/healthz`

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
- `MCP_AUTH_TOKEN`: **required.** Protects the MCP endpoint and the Pi API with `Authorization: Bearer <token>` and is the default token for the Cursor API. The server refuses to start without it — there is no unauthenticated mode.
- `SANDBOX_ROOT`: workspace root for file access and command execution, default current directory
- `SANDBOX_SHELL`: shell used by the `ExecCommand` tool, default `bash`
- `MCP_STATELESS`: enable stateless streamable HTTP mode, default `false`
- `MCP_JSON_RESPONSE`: prefer `application/json` responses, default `false`
- `PI_WEB_ENABLED`: run [pi-web](https://github.com/agegr/pi-web) as a supervised child process and reverse-proxy it, default `false`
- `PI_WEB_PASSWORD`: required when `PI_WEB_ENABLED` is set; enables pi-web's built-in HTTP Basic Auth (username is always `pi`)
- `PI_WEB_PORT`: internal port pi-web listens on (loopback only), default `30141`
- `PI_API_ENABLED`: expose the ConnectRPC [pi session API](#pi-api) at `/butterbox.pi.v1.PiService/`, default `false`
- `PI_API_MAX_SESSIONS`: cap on concurrently active pi session processes, default `8` (the short-lived process behind a session-less `GetAvailableModels` is not counted)
- `PI_API_IDLE_TIMEOUT`: stop a session process after this much inactivity (Go duration, session file is kept and re-attached on next use), default `30m`
- `PI_BIN`: pi executable, default `pi`
- `PI_SESSION_DIR`: override pi's session storage directory (passed as `--session-dir`)
- `CURSOR_API_ENABLED`: expose the ConnectRPC Cursor session API, default `false`
- `CURSOR_API_KEY`: Cursor API key passed to `cursor-sdk-bridge`; never returned by ButterBox
- `CURSOR_AUTH_TOKEN`: optional bearer token dedicated to the Cursor API; defaults to `MCP_AUTH_TOKEN`
- `CURSOR_MAX_SESSIONS`: cap on active Cursor bridge processes, default `PI_API_MAX_SESSIONS` or `8`
- `CURSOR_API_IDLE_TIMEOUT`: stop an idle Cursor bridge while retaining agent state, default `30m`
- `CURSOR_SDK_BRIDGE_BIN`: bridge executable, default `cursor-sdk-bridge`

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

When `PI_API_ENABLED=true`, ButterBox exposes the [pi coding agent](https://github.com/earendil-works/pi) as an agent runtime over ConnectRPC. The service is defined in [`proto/butterbox/pi/v1/pi.proto`](proto/butterbox/pi/v1/pi.proto) and mounted at `/butterbox.pi.v1.PiService/`, protected by the same required `MCP_AUTH_TOKEN` bearer auth as the MCP endpoint (see [Environment Variables](#environment-variables)).

Each session maps to a supervised `pi --mode rpc` child process speaking pi's JSONL RPC protocol. Session IDs are pi's own session IDs and session data lives in pi's session directory, so idle sessions are stopped and transparently re-attached on next use — and every API-driven session shows up in pi-web when that is enabled too. Extension UI dialogs are auto-cancelled so a headless run can never wedge.

`CreateSession` accepts an optional `cwd` — the working directory for the pi process, absolute or relative to `SANDBOX_ROOT`. It must resolve inside the sandbox root and exist, or the call fails with `invalid_argument`; empty keeps the server's own working directory. The effective directory is reported back on the `Session` message, and re-attached sessions are respawned in the session's recorded cwd (read back from pi's session file header when needed).

RPCs: `CreateSession`, `ListSessions`, `GetSession`, `GetAvailableModels` (models pi can use, with provider, modalities, limits, and USD-per-million-token costs), `ListDirectories` (browse the sandbox root for a working-directory picker), `SendMessage` (unary, returns after the run fully settles), `SubmitMessage` + `GetTurn` (async: submit returns immediately with a turn cursor; poll or long-poll for the result — see below), `StreamMessage` (server stream of raw pi events, ends with `agent_settled`), `AbortSession`, `DeleteSession` (`purge: true` also removes the session file).

`GetAvailableModels` works with or without a session. With `session_id` set it asks that session's process (re-attaching it if idle), since a session's scoped model config may differ. With `session_id` empty it answers for the box: a short-lived `pi --mode rpc --no-session` process reads the catalog and is torn down, and the result is cached for five minutes so repeated dropdown loads cost one spawn. That transient process is outside the `PI_API_MAX_SESSIONS` accounting — it holds no session and lives for a single call — so a box at its session cap still answers catalog queries; if the transient process cannot start at all, the last known catalog is served rather than an error.

`ListDirectories` lists the immediate subdirectories of one directory for a working-directory picker: `path` empty means `SANDBOX_ROOT`, anything else must resolve inside it (same rules as `cwd` and the MCP tools) and be an existing directory, or the call fails with `invalid_argument`. One level per call, no recursion and no file contents; dot-directories are skipped unless `include_hidden` is set, symlinked directories are never reported, and a listing over 500 entries comes back with `truncated: true`. Each entry's `path` is absolute, so it feeds straight back in as the next `ListDirectories` path or as `CreateSession.cwd`.

For long runs prefer the async pair over `SendMessage`: `SubmitMessage` detaches the run from the request lifetime (a dropped connection never aborts it; `AbortSession` is the only way to cancel) and returns pi's entries cursor at submit time, which is stable across process restarts. `GetTurn` with `wait_seconds: 0` is a pure poll; `wait_seconds > 0` long-polls up to 30s. Completion is judged from the session entries after the cursor, so a box restart mid-run reports an honest "did not finish" (`running: false` with no `result`) instead of a stale previous answer. Submitting while a run is in flight returns the same busy error as `SendMessage`.

ConnectRPC speaks plain JSON over HTTP POST, so curl works:

```bash
curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/CreateSession \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-task","provider":"anthropic","cwd":"my-repo"}'

curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/SendMessage \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"<id>","message":"What files are here?"}'

# Populate a model dropdown and a cwd picker before any session exists.
curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/GetAvailableModels \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' -d '{}'

curl -X POST http://127.0.0.1:8080/butterbox.pi.v1.PiService/ListDirectories \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' -d '{"path":""}'
```

pi uses its own model credentials (`~/.pi/agent` auth or provider environment variables such as `ANTHROPIC_API_KEY`); make sure they are present in the container environment or persisted home volume.

## Cursor API

When `CURSOR_API_ENABLED=true`, ButterBox exposes the ConnectRPC `butterbox.cursor.v1.CursorService` at `/butterbox.cursor.v1.CursorService/`. It uses the same bearer-auth boundary as the MCP and Pi APIs, or `CURSOR_AUTH_TOKEN` when a dedicated token is configured. The service contract is defined in [`proto/butterbox/cursor/v1/cursor.proto`](proto/butterbox/cursor/v1/cursor.proto).

The container includes the pinned `cursor-sdk-bridge` `v1.0.31` release for Linux amd64 and arm64. Each Cursor session owns one supervised bridge process. The bridge's durable state is retained under the ButterBox user's home directory when the process is stopped for idle timeout or aborted, and the next request resumes the agent by its returned `session_id`.

`CURSOR_API_KEY` is required by the Cursor bridge for model and agent operations. It is passed explicitly to bridge SDK requests as well as the bridge environment, but is never returned in an RPC response or included in logs. A missing or invalid key returns `Unauthenticated` with a `google.rpc.ErrorInfo` detail whose reason is `CURSOR_API_KEY_MISSING_OR_INVALID`, distinct from an invalid ButterBox bearer token.

The service supports `CreateSession`, `SendMessage`, `AbortSession`, and session-less `ListModels`. `SendMessage` waits for the bridge's terminal run result and returns only final assistant text. Inline image bytes and MIME types are forwarded unchanged. A failed or cancelled run, bridge crash, busy session, capacity limit, or unknown session is returned as an explicit RPC error rather than a stale answer.

Example:

```bash
curl -X POST http://127.0.0.1:8080/butterbox.cursor.v1.CursorService/CreateSession \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"cursor-task","model":"composer-2","mode":"agent","cwd":"my-repo"}'

curl -X POST http://127.0.0.1:8080/butterbox.cursor.v1.CursorService/ListModels \
  -H 'Authorization: Bearer secret-token' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Regenerate the ConnectRPC code after editing a proto with `buf generate` (config in `buf.gen.yaml`, lint with `buf lint`).

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
