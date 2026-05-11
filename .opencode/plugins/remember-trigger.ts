// all-might generated
/**
 * Remember Trigger — OpenCode plugin (All-Might)
 *
 * Nudges the agent to run /remember at the right moments. Does NOT
 * duplicate /remember's logic — it only times the prompt. Scope and
 * writing are delegated entirely to the /remember command, which is
 * the single source of truth for how memory gets written.
 *
 * Events:
 *   session.idle                     — every NUDGE_EVERY turns, queue nudge
 *   experimental.session.compacting  — queue last-chance nudge pre-compaction
 *   session.created / session.deleted — init / cleanup per-session state
 *
 * Hook:
 *   chat.message — inject any queued nudge as a prefix to the next user turn
 */
import type { Plugin } from "@opencode-ai/plugin";

const NUDGE_EVERY = 5;

type State = { idleCount: number; pendingNudge: string | null };
const sessions = new Map<string, State>();

const SHARED_NUDGE = `[Memory Nudge]
Did anything worth remembering happen in the last few turns?
If yes, run /remember (it decides the scope and writes).
If nothing stands out, skip.

Scope reminder: project-wide (portable) -> MEMORY.md;
per-corpus knowledge -> memory/understanding/<workspace>.md;
per-corpus personal state -> memory/<kind>/<workspace>.md;
searchable -> memory/journal/<workspace>/.

If you created a new skill or plugin this session -> append a bullet to memory/skills-log.md
(date . path . why). Self-evolution leaves a trace.`;

function nudgeText(turn: number): string {
  return `[Memory Nudge \u2014 turn ${turn}]\n` + SHARED_NUDGE;
}

function preCompactText(): string {
  return [
    "[Memory Nudge \u2014 pre-compaction]",
    "Conversation is about to be summarised. Last chance before history is",
    "condensed: run /remember for anything worth persisting (user prefs,",
    "corrections, per-corpus discoveries). Delegate scope and writing to",
    "/remember.",
    "",
    SHARED_NUDGE,
  ].join("\n");
}

function ensure(sid: string): State {
  let s = sessions.get(sid);
  if (!s) {
    s = { idleCount: 0, pendingNudge: null };
    sessions.set(sid, s);
  }
  return s;
}

export const RememberTriggerPlugin: Plugin = async () => {
  return {
    event: async ({ event }: { event: any }) => {
      const sid = event?.properties?.sessionID ?? "";
      if (!sid) return;
      const type = event?.type;

      if (type === "session.idle") {
        const s = ensure(sid);
        s.idleCount += 1;
        if (s.idleCount % NUDGE_EVERY === 0) {
          s.pendingNudge = nudgeText(s.idleCount);
        }
      } else if (type === "session.created") {
        sessions.set(sid, { idleCount: 0, pendingNudge: null });
      } else if (type === "session.deleted") {
        sessions.delete(sid);
      }
    },

    "chat.message": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      const s = sessions.get(sid);
      if (!s?.pendingNudge) return;
      if (!Array.isArray(output?.parts)) return;
      // Each Part requires id / sessionID / messageID (see OpenCode's
      // TextPart schema in session/message-v2.ts); omitting them makes
      // SyncEvent.run reject the mutated part with "sessionID required".
      const mid = output?.message?.id;
      if (!mid) return;
      const nudge = s.pendingNudge;
      s.pendingNudge = null;
      output.parts.unshift({
        id: "prt_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10),
        sessionID: sid,
        messageID: mid,
        type: "text",
        text: nudge,
        synthetic: true,
      });
    },

    // Pre-compaction hook: inject the scope reminder directly into the
    // compaction prompt so the generated summary carries the framing.
    "experimental.session.compacting": async (input: any, output: any) => {
      const sid = input?.sessionID;
      if (!sid) return;
      if (!output) return;
      const context = output.context ?? (output.context = []);
      if (Array.isArray(context)) {
        context.push(preCompactText());
      }
    },
  };
};

export default RememberTriggerPlugin;
