// all-might generated
/**
 * Role Loader — OpenCode plugin (All-Might)
 *
 * Primes the agent's context with each personality's ROLE.md at the
 * start of every (un-primed) session, and re-primes after compaction
 * — compaction summarises history and dilutes the role description,
 * so a fresh injection keeps each personality's identity stable.
 *
 * Events:
 *   session.created    -> mark session un-primed
 *   session.compacted  -> mark session un-primed (re-inject next message)
 *   session.deleted    -> drop state
 *
 * Hook:
 *   chat.message -> inject ROLE.md prefix once per (un-primed) session
 */
import type { Plugin } from "@opencode-ai/plugin";
import { readFileSync, existsSync, readdirSync, statSync } from "fs";
import { join } from "path";

const primed = new Set<string>();

function readAllRoles(cwd: string): string {
  const personalitiesDir = join(cwd, "personalities");
  if (!existsSync(personalitiesDir)) return "";
  const parts: string[] = [];
  let entries: string[] = [];
  try {
    entries = readdirSync(personalitiesDir).sort();
  } catch {
    return "";
  }
  for (const name of entries) {
    const rolePath = join(personalitiesDir, name, "ROLE.md");
    if (!existsSync(rolePath)) continue;
    let stat;
    try {
      stat = statSync(rolePath);
    } catch {
      continue;
    }
    if (!stat.isFile()) continue;
    try {
      parts.push(`--- Role: ${name} (ROLE.md) ---`);
      parts.push(readFileSync(rolePath, "utf-8"));
      parts.push(`--- End Role: ${name} ---`);
      parts.push("");
    } catch {
      // ignore unreadable role files
    }
  }
  return parts.join("\n");
}

export const RoleLoadPlugin: Plugin = async ({ directory }: any) => {
  const cwd = (directory as string | undefined) ?? process.cwd();

  return {
    event: async ({ event }: { event: any }) => {
      const type = event?.type;
      const sid = event?.properties?.sessionID ?? "";
      if (!sid) return;
      if (
        type === "session.created" ||
        type === "session.compacted" ||
        type === "session.deleted"
      ) {
        primed.delete(sid);
      }
    },

    "chat.message": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      if (primed.has(sid)) return;

      const text = readAllRoles(cwd);
      if (!text.trim()) return;

      const mid = output?.message?.id;
      if (!mid) return;
      if (!Array.isArray(output?.parts)) return;
      output.parts.unshift({
        id: "prt_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10),
        sessionID: sid,
        messageID: mid,
        type: "text",
        text,
        synthetic: true,
      });
      primed.add(sid);
    },
  };
};

export default RoleLoadPlugin;
