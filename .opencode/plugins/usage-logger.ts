// all-might generated
/**
 * Usage Logger — OpenCode plugin (All-Might)
 *
 * Real-time appends to memory/usage.log. Independent of the agent
 * remembering to log inside /remember and /recall — the file is
 * updated as events happen, not at end-of-action.
 *
 * Captured:
 *   - chat.message       — detects /remember and /recall in user text
 *   - tool.execute.after — detects writes under memory/ via Write/Edit
 *
 * appendFileSync flushes synchronously, so every entry hits disk
 * before the hook returns.
 */
import type { Plugin } from "@opencode-ai/plugin";
import { appendFileSync, existsSync, mkdirSync, readdirSync } from "fs";
import { join, dirname } from "path";

const COMMAND_RE = /(?:^|\s)\/(remember|recall)\b/;

function findMemoryDirs(cwd: string): string[] {
  const out: string[] = [];
  const personalitiesDir = join(cwd, "personalities");
  if (existsSync(personalitiesDir)) {
    let entries: string[] = [];
    try { entries = readdirSync(personalitiesDir); } catch { entries = []; }
    for (const name of entries.sort()) {
      const memDir = join(personalitiesDir, name, "memory");
      if (existsSync(memDir)) out.push(memDir);
    }
  }
  if (out.length === 0) out.push(join(cwd, "memory"));
  return out;
}

function append(cwd: string, line: string): void {
  const suffix = line.endsWith("\n") ? line : line + "\n";
  for (const memDir of findMemoryDirs(cwd)) {
    const p = join(memDir, "usage.log");
    try {
      mkdirSync(dirname(p), { recursive: true });
      appendFileSync(p, suffix);
    } catch {
      // Best-effort: a plugin hook must never throw.
    }
  }
}

function nowIso(): string {
  return new Date().toISOString();
}

function extractUserText(parts: unknown): string {
  if (!Array.isArray(parts)) return "";
  const texts: string[] = [];
  for (const p of parts as any[]) {
    if (p?.type === "text" && typeof p.text === "string") texts.push(p.text);
  }
  return texts.join("\n");
}

const MEMORY_PATH_RE = /(?:^|\/)memory\//;

export const UsageLoggerPlugin: Plugin = async ({ directory }: any) => {
  const cwd = (directory as string | undefined) ?? process.cwd();

  return {
    "chat.message": async (input: any, _output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const text = extractUserText(input?.parts);
      if (!text) return;
      const m = text.match(COMMAND_RE);
      if (!m) return;
      const cmd = m[1];
      append(cwd, `${nowIso()} ${cmd} invoked session=${String(sid).slice(0, 8)}`);
      void _output;
    },

    "tool.execute.after": async (input: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const tool = String(input?.tool ?? "");
      if (tool !== "Write" && tool !== "Edit") return;
      const args = input?.args ?? {};
      const filePath: string =
        (typeof args?.file_path === "string" && args.file_path) ||
        (typeof args?.filePath === "string" && args.filePath) ||
        "";
      if (!filePath) return;
      if (!MEMORY_PATH_RE.test(filePath) && !filePath.endsWith("MEMORY.md")) return;
      append(cwd, `${nowIso()} memory-write tool=${tool} file=${filePath}`);
    },
  };
};

export default UsageLoggerPlugin;
