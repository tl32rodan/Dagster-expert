// all-might generated
/**
 * Memory History — OpenCode plugin (All-Might)
 *
 * Post-turn / session-boundary auto-snapshot of personality memory
 * data into .allmight/memory-history/. Backs accidental-delete
 * recovery via `allmight memory restore`.
 *
 * Hooks:
 *   - chat.message               — granular per-turn snapshot
 *   - experimental.session.compacting — pre-compaction fallback
 *   - session.deleted            — final session-end fallback
 *
 * Spawns `allmight memory snapshot --trigger=... --session-id=...`
 * fire-and-forget. Errors are swallowed (plugin must not block).
 */
import type { Plugin } from "@opencode-ai/plugin";
import { spawn } from "child_process";

function snapshot(cwd: string, trigger: string, sid?: string): void {
  const args = ["memory", "snapshot", `--trigger=${trigger}`];
  if (sid) args.push(`--session-id=${sid}`);
  try {
    const child = spawn("allmight", args, {
      cwd,
      stdio: "ignore",
      detached: true,
    });
    // Detach so the plugin returns immediately; the snapshot runs
    // in the background. unref() lets the parent exit independently.
    child.unref();
    child.on("error", () => {
      // allmight not on PATH or other spawn failure — silent.
    });
  } catch {
    // Best-effort: a plugin must never throw.
  }
}

export const MemoryHistoryPlugin: Plugin = async ({ directory }: any) => {
  const cwd = (directory as string | undefined) ?? process.cwd();

  return {
    "chat.message": async (input: any, _output: any) => {
      const sid = input?.sessionID;
      snapshot(cwd, "chat-message", sid);
      void _output;
    },

    "experimental.session.compacting": async (input: any, _output: any) => {
      const sid = input?.sessionID;
      snapshot(cwd, "session-compacting", sid);
      void _output;
    },

    event: async ({ event }: any) => {
      const type = String(event?.type ?? "");
      if (type === "session.deleted") {
        const sid = event?.properties?.sessionID;
        snapshot(cwd, "session-deleted", sid);
      }
    },
  };
};

export default MemoryHistoryPlugin;
