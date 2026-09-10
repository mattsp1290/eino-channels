# .agents

Shared, tool-neutral material for coding agents. Repository conventions are
in the root `AGENTS.md`; read it first.

- `plans/slack-discord-conversations/`: the grounded implementation plan for
  the current service. `00-overview.md` states scope and decisions,
  `01`–`05` the work packages, `06-execution-handoff.md` the current status,
  the refinements made during implementation, and what is still not run.
  Update `06` when status changes; do not rewrite history in `00`–`05`.
- `skills/next-milestone/`: the skill for choosing the next bounded milestone
  from repository evidence and comparator research. Use it with
  `$implementation-plan $next-milestone` before starting a new plan; it must
  not implement anything itself.

Conventions for new plans: one directory per plan under `plans/`, numbered
files, an overview with a document map, and a handoff file that records
verification evidence and deviations. Upstream requests to sibling
repositories follow `skills/next-milestone/references/upstream-request-protocol.md`.
