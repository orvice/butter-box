import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createConnection, type Socket } from "node:net";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

// The JSON contract is shared, versioned and hashed, not reconstructed here.
const MAX_PAYLOAD = 256 * 1024;
const MAX_FRAME = 2 * MAX_PAYLOAD + 8 * 1024;
interface Wire {
  type: string;
  id?: string;
  turn_id?: string;
  call_id?: string;
  tools?: string[];
  deadline_ms?: number;
  result?: Record<string, unknown>;
  error?: string;
}
interface Grant { turnID: string; deadline: number }
const beginKey = (id: string) => `begin:${id}`;
const callKey = (turnID: string, callID: string) => `call:${turnID}:${callID}`;

export default function (pi: ExtensionAPI) {
  const socketPath = process.env.BUTTERBOX_PI_HOST_SOCKET;
  const contractPath = process.env.BUTTERBOX_PI_HOST_CONTRACT;
  // Don't leak the connection configuration into tools' child processes.
  delete process.env.BUTTERBOX_PI_HOST_SOCKET;
  delete process.env.BUTTERBOX_PI_HOST_CONTRACT;
  if (!socketPath || !contractPath) return;
  const rawContract = readFileSync(contractPath);
  const contract = JSON.parse(rawContract.toString("utf8")) as {
    name: string; label: string; description: string; parameters: Record<string, unknown>;
  };
  const hash = createHash("sha256").update(rawContract).digest("hex");
  let socket: Socket | undefined;
  let grant: Grant | undefined;
  let expiry: ReturnType<typeof setTimeout> | undefined;
  const pending = new Map<string, { resolve: (msg: Wire) => void; reject: (error: Error) => void }>();

  const hide = () => pi.setActiveTools(pi.getActiveTools().filter((name) => name !== contract.name));
  const reset = (reason: string) => {
    grant = undefined;
    clearTimeout(expiry);
    hide();
    for (const waiter of pending.values()) waiter.reject(new Error(reason));
    pending.clear();
  };
  const send = (message: object) => {
    if (!socket || socket.destroyed) throw new Error("Box host relay disconnected");
    const frame = JSON.stringify(message);
    if (Buffer.byteLength(frame) > MAX_FRAME) throw new Error("Host relay frame too large");
    socket.write(frame + "\n");
  };
  const request = (key: string, message: object, deadline: number, signal?: AbortSignal, onAbort?: () => void): Promise<Wire> => {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) { reject(new Error("Host tool aborted")); return; }
      if (pending.has(key)) { reject(new Error("Duplicate host call ID")); return; }
      const finish = (error?: Error, reply?: Wire) => {
        pending.delete(key);
        clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
        if (error) reject(error); else resolve(reply!);
      };
      const abort = () => {
        try { onAbort?.(); } catch { /* disconnected */ }
        finish(new Error("Host tool aborted or expired"));
      };
      const timer = setTimeout(abort, Math.max(0, deadline - Date.now()));
      pending.set(key, { resolve: (reply) => finish(undefined, reply), reject: (error) => finish(error) });
      signal?.addEventListener("abort", abort, { once: true });
      try { send(message); } catch (error) { finish(error instanceof Error ? error : new Error(String(error))); }
    });
  };
  const authorized = () => {
    if (!grant || grant.deadline <= Date.now() || !socket || socket.destroyed) throw new Error("render_ui is not authorized for this turn");
    return grant;
  };

  pi.registerTool({
    name: contract.name,
    label: contract.label,
    description: contract.description,
    parameters: Type.Unsafe<Record<string, unknown>>(contract.parameters),
    exposure: "model-only",
    executionMode: "sequential",
    async execute(callID, args, signal) {
      const current = authorized(); // Required even for historical/nested calls.
      if (Buffer.byteLength(JSON.stringify(args)) > MAX_PAYLOAD) throw new Error("Host tool arguments exceed 256 KiB");
      const reply = await request(callKey(current.turnID, callID), {
        type: "call", turn_id: current.turnID, call_id: callID, tool: contract.name, arguments: args,
      }, current.deadline, signal, () => send({ type: "cancel_call", turn_id: current.turnID, call_id: callID }));
      if (reply.error) throw new Error(reply.error); // Ordinary Pi tool error, not transport success.
      return { content: [{ type: "text", text: JSON.stringify(reply.result) }], details: reply.result };
    },
  });
  pi.on("tool_call", (event) => {
    if (event.toolName !== contract.name) return;
    try { authorized(); } catch (error) { return { block: true, reason: String(error) }; }
  });

  pi.on("session_start", async (_event, ctx) => {
    reset("Session initialized");
    if (ctx.mode !== "rpc") return;
    socket?.destroy();
    await new Promise<void>((resolve, reject) => {
      const connection = createConnection(socketPath);
      socket = connection;
      let buffer = Buffer.alloc(0);
      connection.once("error", reject);
      connection.on("error", () => reset("Box host relay disconnected"));
      connection.on("close", () => reset("Box host relay disconnected"));
      connection.on("data", (chunk: Buffer) => {
        buffer = Buffer.concat([buffer, chunk]);
        if (buffer.length > MAX_FRAME) { connection.destroy(); return; }
        let end: number;
        while ((end = buffer.indexOf(10)) !== -1) {
          const line = buffer.subarray(0, end); buffer = buffer.subarray(end + 1);
          try {
            const message = JSON.parse(line.toString("utf8")) as Wire;
            const key = message.type === "config" ? beginKey(message.id ?? "") : callKey(message.turn_id ?? "", message.call_id ?? "");
            pending.get(key)?.resolve(message);
          } catch { connection.destroy(); return; }
        }
      });
      connection.once("connect", () => {
        connection.removeListener("error", reject);
        send({ type: "hello", version: 1, hash, session_id: ctx.sessionManager.getSessionId() });
        resolve();
      });
    });
  });
  pi.on("before_agent_start", async () => {
    reset("New run requires fresh authorization");
    if (!socket || socket.destroyed) return;
    const id = randomUUID();
    const config = await request(beginKey(id), { type: "begin", id }, Date.now() + 5000);
    if (!config.turn_id || !config.tools?.includes(contract.name) || !config.deadline_ms || config.deadline_ms <= Date.now()) return;
    grant = { turnID: config.turn_id, deadline: config.deadline_ms };
    expiry = setTimeout(() => reset("Host authorization expired"), config.deadline_ms - Date.now());
    pi.setActiveTools([...pi.getActiveTools(), contract.name]);
  });
  pi.on("agent_settled", () => reset("Run settled"));
  pi.on("session_shutdown", () => { reset("Session shut down"); socket?.destroy(); });
}
