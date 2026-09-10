# Upstream requests and resume gate

Use local project records under `~/.agents/projects/` unless the user supplies another root. Requests are local coordination artifacts; this protocol does not authorize contacting maintainers or modifying sibling application code.

## Resume before selection

Scan project `requests/` records for exact `Blocker consumer: eino-channels`, accepting Markdown emphasis/backticks around fields and scalar values. Also inspect requests explicitly marked blocking in active eino-channels plans: legacy records may lack this metadata. Missing, conflicting, or malformed metadata does not silently clear a known blocker. Incoming requests addressed to eino-channels are demand evidence and do not automatically block selection.

Match each request to its same-filename `responses/` record. Recognize `open`, `resolved`, `withdrawn`, and `superseded` statuses. Verify a resolved claim against the response, committed target public API, relevant tests, and consumable version/commit. A response, branch, or worktree implementation alone is insufficient. Follow linked prerequisites if present; report cycles or incomplete dependency sets as unresolved.

Report all unresolved outbound blockers together: selected milestone, target, absolute request path, evidence still missing, and exact unblock action. Stop before new candidates or plans. If the user explicitly withdraws or supersedes the blocked milestone, retain that decision and record status/history when authorized; do not require them to authorize the same action twice. A declined request returns the selected milestone to a scope/ownership decision, not a silent local workaround.

## After selection

Request a contract only if the chosen acceptance journey requires it, current public target APIs cannot provide it, and a local implementation would duplicate target ownership or rely on private/speculative interfaces. Complete the ownership map before writing; one record per distinct target-owned contract, never bundle different repositories.

Verify target checkout/module identity, revision, worktree state, public APIs, tests, plans, and publication state. Search target requests and responses for equivalent needs. Reuse matching eino-channels records; link other consumers' equivalent requests without editing them. Verify actual response code even when an older request still says open.

Propose the exact destination:

`~/.agents/projects/<verified-target>/requests/YYYY-MM-DD-<contract-slug>.md`

Create local records when the user's task/session authorizes cross-repository request filing. Otherwise prepare the complete request in conversation and ask only for the missing write authorization. Do not treat skill invocation alone as permission for external messages. Use safe local file editing, validate destination containment and reject symlink escapes, and never overwrite a pre-existing request. If another process created it, inspect and deduplicate.

Include these interoperable fields and sections:

```markdown
# Request: <public contract>

- **Requested by:** `eino-channels` next-milestone selection
- **Blocker consumer:** `eino-channels`
- **Request status:** `open`
- **Date:** YYYY-MM-DD
- **Selected milestone:** <name>
- **Target repo:** <verified identity and checkout>
- **Pinned commit under evaluation:** <full SHA>
- **Consumer:** `eino-channels`

## Background
<Bounded journey, verified ownership, current evidence, missing seam.>

## Ask
<Smallest required public behavior; proposed API shapes labeled as proposals.>

## Out of scope
<Channel-owned work and unrelated capabilities.>

## Acceptance
<Public contract, relevant failure/concurrency/recovery tests, compatibility,
documentation, and a consumable immutable pin.>

## Response and unblock contract
<Same filename under target responses/; accepted contract and implementation
pin to verify, or decline with reason/supported alternative.>

## References
<Exact paths/symbols, related requests and prerequisite records, official URLs.>

## Status history
- YYYY-MM-DD — open: created for <milestone>.
```

After creating or reusing requests, mark the milestone `blocked upstream`, show every request and unblock condition, and stop before planning or implementation. Unfiled required requests also block; lack of authorization is not a cleared dependency. Never bypass with a local duplicate, private import, fork, or fake success.

On verified completion, record response, pin, date, and status history when authorized. Preserve historical content. Withdrawal/supersession requires an explicit user decision and its reason; do not delete records or alter another consumer's status. Then refresh local and external evidence before continuing the selected milestone or fresh selection as appropriate.
