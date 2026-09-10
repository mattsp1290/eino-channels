# Channel capability frontier

Build a compact matrix with current state, exact local evidence, public dependency/owner, evidence from each comparator (or documented unknown/irrelevance), gap, and relevance to the next milestone. Assess affected lanes; do not require all of them in one increment.

| Lane | Behaviors to examine |
| --- | --- |
| Installation and operations | Bot setup, scopes, configuration, health, reconnection, shutdown, upgrade and deployability |
| Participation | Mentions, DMs, thread subscription, commands, silence rules, bot loops, edits/deletions |
| Identity and context | Workspace/guild/channel/thread/user mapping, membership, permitted history, attachments, isolation |
| Conversation continuity | Multi-turn context, durable admission, duplicate ingress, restart, replay, session reset |
| Delegated work | Task acceptance, execution routing, progress, cancellation, concurrent messages, results and links |
| Tools and approvals | Scoped connections, acting account, approval identity, expiry, resume, write-effect recovery |
| Native results | Streaming edits, chunking, cards, artifacts, citations, safe formatting and private delivery |
| Customization | Agent/persona/model/tool selection and reusable adapter seams justified by real consumers |
| Reliability and bounds | Backpressure, quotas, rate limits, ambiguous delivery, offline gaps, resource limits |
| Governance | Access policy, tenant separation, untrusted content, secret handling, redaction, audit and retention |

Show the verified current flow, relevant reference lessons, and each candidate's smallest before/after flow. Label missing and proposed seams. Separate platform receipt, runtime admission, execution, and output delivery, including failure/recovery boundaries.

## Candidates

Each option must state:

- Name and type: `bootstrap application` or `add capability`.
- Primary user and one observable conversation or operator outcome.
- Scope, non-goals, affected platforms, and explicit comparator parity limits.
- Exact current evidence or proposed insertion points, and relationship to existing plans.
- Relevant lessons and intentional differences from the three references.
- Public Eino contracts and component ownership; suspected upstream gaps.
- Readiness: `ready`, `foundation first`, `decision needed`, `upstream request likely`, or `discovery`.
- Before/after flow, largest dependency or decision, acceptance approach, and ranking rationale.

List `planned` outcomes outside the options. `Blocked upstream` is a gate, not a request-only candidate. Do not offer an internal refactor, broad parity roadmap, static bot mock, or second copy of an existing plan as a user milestone. An independently useful operational foundation is valid if it removes an evidenced blocker and has observable acceptance.

Rank by nearest complete useful journey, fit with public Eino ownership, reliable shared-conversation behavior, credential-free verifiability, and reduced uncertainty for subsequent work. No arbitrary numeric scoring. Preserve the user's different preference.

## Material decisions and proof

Resolve only decisions affected by the selection: participation/history policy, shared versus personal identity, execution location, tool grants, approval authority, result audience, persistence/retention, platform coverage, deployment, and compatibility. Respect established decisions; do not silently inherit a competitor's policy.

For a conversation milestone, acceptance should cover a real runtime-backed exchange and follow-up, correct routing, interruption/error, and restart/duplicate behavior where relevant. For a tool milestone, add authorization and durable approval/resume tests. Keep fake platform transport, scripted model through real runtime, and authorized live tests as distinct evidence. Never claim a real installation or remote effect from a fixture.
