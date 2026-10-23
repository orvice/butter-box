# Pi host tools protocol v1

Issue: https://github.com/orvice/butter-box/issues/29

## Negotiation and submission

Deploy Box first, then enable the Butter integration after capability discovery. Upgrading Pi alone does not enable A2UI. `GetCapabilities` advertises `host_tools_protocol_version=1`, the SHA-256 of the bundled `render_ui` contract, and limits without starting Pi. With `session_id`, it also attaches the target session and checks its live extension handshake. Require `host_tools_extension_ready=true` and a matching contract hash before enabling. Submit checks readiness again; a static capability is not proof of readiness. Missing/false/unimplemented capabilities mean unsupported, never silently enabled.

`SubmitMessage.host_tools` opts in for this submission only: version 1, the exact contract hash, `allowed_tools=["render_ui"]`, and `max_run_seconds` (1..86400, default 1800). No other tools or presentation overrides are supported in v1. Butter owns catalog/policy/presentation validation; the contract accepts `surface_id?`, `messages`, and `fallback?` unchanged. The bundled JSON contract is the source for extension parameters and description. Butter must compare its pinned hash; changing the contract requires an explicit client update/version negotiation.

Box generates a cryptographically random `turn_id`, unrelated to the transcript cursor. Model selection, authorization, cursor capture and prompt submission share the existing run slot. Failed setup revokes authorization; unresolved prompt acknowledgement keeps supervision and the authorization deadline. Never retry prompts automatically. Omitted host configuration preserves legacy behavior, including Send/Stream, Linear and ordinary pi-web. Authorization is never restored from history.

## Reading and responding

`ListHostCalls(session_id, turn_id, wait_seconds)` is independent of `GetTurn`: 0 polls, 1..30 long-polls. It wakes on pending work, revocation or completion, not just `agent_settled`. It returns all outstanding requests with Box-bound session/turn identity, Pi call ID, tool name and JSON arguments. Reads are non-consuming. Cancelling/disconnecting a read never cancels the run. `running=false` means this relay grant ended, not that a card was created. Final text and transcript cursors retain their existing meaning.

`RespondHostCall` carries exactly one of `result_json` (a JSON object, e.g. `{"surface_id":"s1","revision":1,"status":"created"}`) or `error` (nonempty ordinary tool error). Box does not interpret card data. Success reaches the original Pi tool only after this response. A tool error becomes a failed Pi tool result; the model can correct parameters and call again. Box never fabricates card success.

A repeated receipt with the same JSON/error is idempotent (JSON whitespace, string escapes and object key order are normalized; numeric lexemes are preserved to avoid precision loss). A different receipt is `already_exists`. Unknown calls/turns/sessions and receipts invalidated by cancellation/expiry return `not_found` or `failed_precondition`, never route to a newer turn. Completed, acknowledged receipts remain retryable for 10 minutes after grant completion, until process exit/deletion, or until the session's 8 retained grants are exceeded (oldest completed first). Unacknowledged requests are discarded on revocation. Limits are advertised; clients must not assume retention after restart.

## Bounds and lifecycle

- One pending call per session; `render_ui` uses Pi sequential execution. Excess parallel calls receive a tool error rather than allocate a queue.
- At most 64 calls per turn, call IDs at most 256 bytes, arguments and successful results at most 256 KiB each, tool errors at most 8 KiB.
- At most 8 retained grants per active process; completed grants expire after 10 minutes and are collected lazily on relay operations/new submissions. Active sessions are bounded by the existing session cap.
- Grant lifetime covers setup and all model/tool work, not just host waiting. The host must choose a deadline aligned with its own max-run deadline (default 30 minutes, maximum 24 hours). There is no renewal in v1. A lost host cannot retain capacity forever. At expiry Box revokes pending calls and stops that Pi process, interrupting even unresponsive preflight/control operations. Session files survive; the turn is not automatically retried or resumed.
- `AbortSession` immediately revokes pending host calls, then uses existing native abort barriers, including cancellation before prompt acceptance. Authorization ends on rejection, complete settlement, explicit cancellation, expiry or process exit. Slot supervision remains unchanged for an unconfirmed native abort.
- Pi/Box restart loses in-memory promises, authorization and receipts. Old IDs cannot route to new processes. Existing `GetTurn`/entries recovery is best-effort transcript recovery, not a host-call recovery or an exactly-once guarantee. A host-tool turn with only a `toolUse` assistant message is not a completed final answer.

Butter must persist cards and its deduplicated result before replying. If acknowledgement is uncertain, retry only reads/identical receipts within retention. No cross-system exactly-once guarantee is made.

## Trusted local adapter

Box embeds and extracts its versioned extension and contract into a private per-process temporary directory, loads it with Pi's explicit `--extension`, and removes it on process exit. The extension connects to a private Unix domain socket on `session_start` (also on attach), exchanges protocol/hash/session identity, and closes on shutdown. No custom records are written to Pi stdout. A missing/incompatible extension fails host-enabled submission before prompt delivery; legacy requests can still run. The adapter requires Pi >=1.2.0; the deterministic suite has been run on 1.2.0 and 1.3.0.

The socket uses bounded LF-delimited JSON: `hello`, `begin`/`config`, `call`/`result`, and `cancel_call`. At `before_agent_start` the extension obtains a single-use grant from Box, sets model-visible tools, and checks authorization again at tool execution. Both sides enforce expiry/turn binding. On settlement it hides the tool; the next run fetches fresh authorization even if previous cleanup raced. Local transport loss fails closed and stops an authorized run. This is isolation between API runs, not a sandbox against malicious same-UID code: Pi extensions/tools have the Box user's OS permissions. Load only trusted extensions.

## Verification

The ConnectRPC boundary tests cover compatibility, validation, isolation, retries, cancellation and limits. An opt-in deterministic real-Pi test uses a local OpenAI-compatible provider, the bundled extension and a fake Butter host; it requires no paid model or external credentials. It verifies pending calls before settlement, host errors and correction, successful results, repeat reads/receipts, attach, and cleanup. Existing submission/model/concurrency regression tests remain mandatory.
