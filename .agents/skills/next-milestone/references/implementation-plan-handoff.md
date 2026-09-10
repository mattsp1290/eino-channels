# Resolved milestone handoff

Use only after selection and verification that no upstream dependency remains unresolved. Keep the brief in conversation, not a separate repository artifact.

Include:

- Selected name, type, readiness, primary user, observable outcome, and accepted trade-off.
- Bounded journey, supported platforms, non-goals, and comparator parity limits.
- Repository facts with paths/symbols/tests; existing-plan and incoming-demand relationships.
- Verified dependency owners, public APIs, revisions and consumable pins.
- Current external evidence with direct URLs, publishers, dates/revisions; distinguish inferences and proposals.
- Affected capability matrix, current and target conversation flow, and explicit host/runtime/sibling ownership.
- Functional requirements, relevant operational limits, and component needing detailed design.
- API/configuration, routing/identity/context, persistence and authoritative transcript boundaries.
- Participation, authorization, tool/approval authority, result visibility, and redaction decisions.
- Admission/delivery guarantees, concurrency, cancellation, replay, recovery, rate limiting and teardown.
- Dependencies and execution order; migration/compatibility and rollback or exit seam.
- User decisions and assumptions; unresolved questions with owner and exact unblock action.
- Resolved request/response evidence; unresolved upstream requests must be none.
- Acceptance mapping for each user requirement, meaningful local tests, credential-free runtime path, and separately authorized live checks.

A brief is ready only when material decisions and dependencies are resolved. If the user cannot resolve a decision, keep the selection and mark it blocked with owner/action; do not invent an answer.

When composed with `$implementation-plan`, resume its normal operating-context workflow, reusing still-applicable explicit answers. Derive one safe kebab-case plan name after selection and incorporate the brief into that skill's normal overview, work files, and execution handoff before its required reviews. Do not create a duplicate of an existing plan; resolve whether the user intends a new outcome or an explicit revision. A decision-blocked plan must say so; an upstream-blocked milestone produces no new plan.

When standalone, deliver the brief and tell the user to invoke `$implementation-plan` with the resolved milestone. Do not require repeating discovery and selection already completed in this conversation.
