// all-might generated
/**
 * TODO Curator — OpenCode plugin (All-Might)
 *
 * Strategic-layer task accounting. Complements OpenCode's built-in TODO
 * (tactical, per-session) by tracking TODOs across sessions, scoped per
 * corpus. The agent is never left staring at an empty TODO list on
 * session start — unfinished items from previous sessions surface
 * automatically.
 *
 * Three phases:
 *  1. Observe — tool.execute.after with tool="TodoWrite" captures the
 *               latest TODO array into an in-memory session ledger.
 *  2. Curate  — experimental.session.compacting (and session.deleted)
 *               append a dated section to memory/todos/<workspace>.md
 *               with the session's TODOs.
 *  3. Surface — on first tool call that reveals a workspace, load the
 *               "## Open" section from memory/todos/<workspace>.md and
 *               queue it for injection on the next chat.message.
 *
 * Workspace inference: scans any tool's args for a
 * database/<name>/ path fragment. If never seen this session,
 * curation at session end writes under "unscoped" workspace.
 */
import type { Plugin } from "@opencode-ai/plugin";
import { readFileSync, existsSync, mkdirSync, appendFileSync, readdirSync } from "fs";
import { join, dirname } from "path";

type TodoItem = { id?: string; content: string; status: string };
type Ledger = {
  workspace: string | null;
  latest: TodoItem[];
  pendingSurface: string | null;
};

const sessions = new Map<string, Ledger>();

function ensure(sid: string): Ledger {
  let s = sessions.get(sid);
  if (!s) {
    s = { workspace: null, latest: [], pendingSurface: null };
    sessions.set(sid, s);
  }
  return s;
}

const WORKSPACE_RE = /database\/([^/\s"']+)/;

function inferWorkspace(args: any): string | null {
  if (!args) return null;
  const haystack = typeof args === "string" ? args : JSON.stringify(args);
  const m = haystack.match(WORKSPACE_RE);
  return m?.[1] ?? null;
}

function memoryDirForWorkspace(cwd: string, workspace: string): string {
  const personalitiesDir = join(cwd, "personalities");
  if (existsSync(personalitiesDir)) {
    let entries: string[] = [];
    try { entries = readdirSync(personalitiesDir); } catch { entries = []; }
    // First: a personality that owns this workspace under its database/
    for (const name of entries) {
      if (existsSync(join(personalitiesDir, name, "database", workspace))) {
        return join(personalitiesDir, name, "memory");
      }
    }
    // Fallback: first personality with a memory/ subdir
    for (const name of entries.sort()) {
      const memDir = join(personalitiesDir, name, "memory");
      if (existsSync(memDir)) return memDir;
    }
  }
  return join(cwd, "memory");
}

function loadOpenBacklog(cwd: string, workspace: string): string | null {
  const path = join(memoryDirForWorkspace(cwd, workspace), "todos", `${workspace}.md`);
  if (!existsSync(path)) return null;
  const content = readFileSync(path, "utf-8");
  const marker = "## Open";
  const openIdx = content.indexOf(marker);
  if (openIdx === -1) return null;
  const rest = content.slice(openIdx + marker.length);
  const nextMatch = rest.match(/\n## /);
  const section = nextMatch ? rest.slice(0, nextMatch.index!) : rest;
  const body = section.trim();
  return body || null;
}

function appendCuration(
  cwd: string,
  workspace: string,
  items: TodoItem[],
): void {
  if (items.length === 0) return;
  const path = join(
    memoryDirForWorkspace(cwd, workspace), "todos", `${workspace}.md`,
  );
  mkdirSync(dirname(path), { recursive: true });
  if (!existsSync(path)) {
    appendFileSync(
      path,
      `# ${workspace} TODOs\n\n## Open\n\n## Done\n\n## Blocked\n`,
    );
  }
  const date = new Date().toISOString().slice(0, 10);
  const lines: string[] = [
    "",
    `## Session ${date}`,
    ...items.map((t) => {
      const mark = t.status === "completed" ? "x" : " ";
      const suffix = t.status === "in_progress" ? "  (in progress)" : "";
      return `- [${mark}] ${t.content}${suffix}`;
    }),
    "",
  ];
  appendFileSync(path, lines.join("\n"));
}

function surfaceText(workspace: string, backlog: string): string {
  return [
    `[TODO Backlog \u2014 ${workspace}]`,
    "Carried over from previous sessions:",
    backlog,
    "",
    "Decide which items to pull into this session's TODO list (via TodoWrite).",
  ].join("\n");
}

export const TodoCuratorPlugin: Plugin = async ({ directory }: any) => {
  const cwd = (directory as string | undefined) ?? process.cwd();

  return {
    event: async ({ event }: { event: any }) => {
      const sid = event?.properties?.sessionID ?? "";
      if (!sid) return;
      const type = event?.type;

      if (type === "session.created") {
        ensure(sid);
      } else if (type === "session.deleted") {
        const s = sessions.get(sid);
        if (s) {
          appendCuration(cwd, s.workspace ?? "unscoped", s.latest);
        }
        sessions.delete(sid);
      }
    },

    "tool.execute.after": async (input: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const s = ensure(sid);

      if (!s.workspace) {
        const ws = inferWorkspace(input?.args);
        if (ws) {
          s.workspace = ws;
          const backlog = loadOpenBacklog(cwd, ws);
          if (backlog) {
            s.pendingSurface = surfaceText(ws, backlog);
          }
        }
      }

      if (input?.tool === "TodoWrite") {
        const todos = input?.args?.todos;
        if (Array.isArray(todos)) {
          s.latest = todos.map((t: any) => ({
            id: t.id,
            content: t.content ?? t.activeForm ?? "",
            status: t.status ?? "pending",
          }));
        }
      }
    },

    "chat.message": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const s = sessions.get(sid);
      if (!s?.pendingSurface) return;
      if (!Array.isArray(output?.parts)) return;
      // Each Part requires id / sessionID / messageID (see OpenCode's
      // TextPart schema in session/message-v2.ts); omitting them makes
      // SyncEvent.run reject the mutated part with "sessionID required".
      const mid = output?.message?.id;
      if (!mid) return;
      const surface = s.pendingSurface;
      s.pendingSurface = null;
      output.parts.unshift({
        id: "prt_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10),
        sessionID: sid,
        messageID: mid,
        type: "text",
        text: surface,
        synthetic: true,
      });
    },

    // Pre-compaction: append session's TODOs to the per-corpus ledger
    // and mention it in the compaction context so the summary doesn't
    // silently lose the curated file reference.
    "experimental.session.compacting": async (input: any, output: any) => {
      const sid = input?.sessionID;
      const s = sid ? sessions.get(sid) : undefined;
      if (!s?.workspace) return;
      appendCuration(cwd, s.workspace, s.latest);
      const context = output?.context ?? (output && (output.context = []));
      if (Array.isArray(context)) {
        const ledger = join(
          memoryDirForWorkspace(cwd, s.workspace),
          "todos", `${s.workspace}.md`,
        );
        context.push(
          `Curated TODO ledger updated at ${ledger} \u2014 ` +
            "reference it instead of duplicating the list in the summary.",
        );
      }
    },
  };
};

export default TodoCuratorPlugin;
