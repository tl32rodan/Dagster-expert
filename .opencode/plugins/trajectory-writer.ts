// all-might generated
/**
 * Trajectory Writer — OpenCode plugin (All-Might, F5)
 *
 * Captures structured session data so future offline analysis
 * (allmight memory export --format jsonl) has something to query.
 * Transparent to the daily user: never injects into the chat; only
 * writes a frontmatter-wrapped markdown file on flush events.
 *
 * Captured per session:
 *   - input     (last user message)
 *   - tool_calls (each {tool, args} from tool.execute.before,
 *                 annotated with verdict from tool.execute.after)
 *   - output    (accumulated agent response summary)
 *   - workspace (inferred from any database/<name>/ path)
 *
 * Flush triggers:
 *   - experimental.session.compacting — last chance before history is summarised
 *   - session.deleted                 — session closed without compaction
 *
 * Hook:
 *   - chat.message — record the latest user input (does NOT mutate output)
 */
import type { Plugin } from "@opencode-ai/plugin";
import { existsSync, mkdirSync, readdirSync, writeFileSync } from "fs";
import { join } from "path";

type ToolCallRec = { tool: string; args: any; verdict: "ok" | "drift" | "blocked" };

type Trajectory = {
  workspace: string | null;
  input: string;
  tool_calls: ToolCallRec[];
  output: string;
  pendingToolIndex: number | null;
};

const sessions = new Map<string, Trajectory>();

const WORKSPACE_RE = /database\/([^/\s"']+)/;

function memoryDirForWorkspace(cwd: string, workspace: string): string {
  const personalitiesDir = join(cwd, "personalities");
  if (existsSync(personalitiesDir)) {
    let entries: string[] = [];
    try { entries = readdirSync(personalitiesDir); } catch { entries = []; }
    for (const name of entries) {
      if (existsSync(join(personalitiesDir, name, "database", workspace))) {
        return join(personalitiesDir, name, "memory");
      }
    }
    for (const name of entries.sort()) {
      const memDir = join(personalitiesDir, name, "memory");
      if (existsSync(memDir)) return memDir;
    }
  }
  return join(cwd, "memory");
}

function ensure(sid: string): Trajectory {
  let t = sessions.get(sid);
  if (!t) {
    t = {
      workspace: null,
      input: "",
      tool_calls: [],
      output: "",
      pendingToolIndex: null,
    };
    sessions.set(sid, t);
  }
  return t;
}

function inferWorkspace(args: any): string | null {
  if (!args) return null;
  const haystack = typeof args === "string" ? args : JSON.stringify(args);
  const m = haystack.match(WORKSPACE_RE);
  return m?.[1] ?? null;
}

function yamlEscape(s: string): string {
  // Block literal (|) keeps newlines verbatim; indent by 2 spaces.
  const indented = s.replace(/\n/g, "\n  ");
  return "|\n  " + indented;
}

function flush(cwd: string, sid: string, t: Trajectory): void {
  if (!t.input && t.tool_calls.length === 0) return;
  const workspace = t.workspace ?? "general";
  const now = new Date();
  const iso = now.toISOString();
  const ts = iso.replace(/[:.]/g, "-");
  const id = `${iso.slice(0, 19)}-${sid.slice(0, 6)}`;
  const dir = join(memoryDirForWorkspace(cwd, workspace), "journal", workspace);
  mkdirSync(dir, { recursive: true });
  const path = join(dir, `${ts}-trajectory.md`);

  const outcome = t.tool_calls.some((c) => c.verdict === "drift" || c.verdict === "blocked")
    ? "partial"
    : "success";

  const toolCallsYaml =
    t.tool_calls.length === 0
      ? "[]"
      : "\n" +
        t.tool_calls
          .map(
            (c) =>
              `  - tool: ${c.tool}\n` +
              `    args: ${JSON.stringify(c.args)}\n` +
              `    verdict: ${c.verdict}`,
          )
          .join("\n");

  const frontmatter =
    "---\n" +
    "allmight_journal: v1\n" +
    `id: ${id}\n` +
    "type: trajectory\n" +
    `workspace: ${workspace}\n` +
    "trigger: auto\n" +
    `input: ${yamlEscape(t.input)}\n` +
    `tool_calls: ${toolCallsYaml}\n` +
    `output: ${yamlEscape(t.output)}\n` +
    `outcome_label: ${outcome}\n` +
    "tags: []\n" +
    "supersedes: null\n" +
    `created_at: ${iso}\n` +
    "---\n";

  const body = `# ${iso.slice(0, 10)} \u2014 session trajectory (${workspace})\n`;
  writeFileSync(path, frontmatter + body);
}

export const TrajectoryWriterPlugin: Plugin = async ({ directory }: any) => {
  const cwd = (directory as string | undefined) ?? process.cwd();

  return {
    event: async ({ event }: { event: any }) => {
      const sid = event?.properties?.sessionID ?? "";
      if (!sid) return;
      const type = event?.type;

      if (type === "session.created") {
        ensure(sid);
      } else if (type === "session.deleted") {
        const t = sessions.get(sid);
        if (t) flush(cwd, sid, t);
        sessions.delete(sid);
      }
    },

    "chat.message": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const t = ensure(sid);
      // Capture the last user message verbatim. Never mutate output.parts
      // here — trajectory writing stays transparent to the chat.
      const parts = input?.parts;
      if (Array.isArray(parts)) {
        const texts = parts
          .filter((p: any) => p?.type === "text" && typeof p.text === "string")
          .map((p: any) => p.text);
        if (texts.length > 0) t.input = texts.join("\n");
      }
      void output;
    },

    "tool.execute.before": async (input: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const t = ensure(sid);
      if (!t.workspace) {
        const ws = inferWorkspace(input?.args);
        if (ws) t.workspace = ws;
      }
      t.tool_calls.push({
        tool: String(input?.tool ?? "unknown"),
        args: input?.args ?? {},
        verdict: "ok",
      });
      t.pendingToolIndex = t.tool_calls.length - 1;
    },

    "tool.execute.after": async (input: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const t = ensure(sid);
      const idx = t.pendingToolIndex;
      if (idx !== null && t.tool_calls[idx]) {
        const verdict = input?.verdict;
        if (verdict === "drift" || verdict === "blocked" || verdict === "ok") {
          t.tool_calls[idx].verdict = verdict;
        }
      }
      t.pendingToolIndex = null;
    },

    "experimental.session.compacting": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const t = sessions.get(sid);
      if (t) {
        flush(cwd, sid, t);
        // Reset captured state so post-compaction continues fresh.
        t.input = "";
        t.tool_calls = [];
        t.output = "";
      }
      void output;
    },
  };
};

export default TrajectoryWriterPlugin;
