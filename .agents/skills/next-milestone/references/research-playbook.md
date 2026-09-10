# Repository and current research

## Local evidence

Resolve the Git root, read applicable AGENTS.md and contributor guidance, and record revision and worktree state. Inspect code, tests, recent history, module pins, quality commands, release state, `.agents/plans/`, and relevant project requests/responses. Do not clean or change these artifacts during discovery.

The initial `slack-discord-conversations` plan is a discovery seed, not a permanent roadmap or proof of implementation. Re-read its current scope, dependency gate, decisions, and execution status. Its no-tools bootstrap and platform choices do not permanently limit this product; extending or superseding them requires an explicit selected outcome. Do not duplicate its claimed work or assume an old blocking dependency is still missing.

Inventory unique resolved `~/git/eino-*` Git roots, distinguish worktrees of the same repository, and inspect relevant siblings' guidance, committed public APIs, tests, consumer docs, plans, and published pins. Likely owners include `eino-agent` (singular), `eino-providers`, `eino-tools`, `eino-agent-extensions`, `eino-agui`, and `eino-obs`. `eino-tui` is a consumer reference for runtime composition and lifecycle, not a package to import through its internals. Deep-inspect only affected contracts.

Classify capabilities as `implemented`, `partial`, `planned`, `absent`, `blocked`, or `unknown`. Require an executable path and meaningful tests or observed behavior for implemented claims. Use exact paths/symbols/tests and separate repository facts, local dependency facts, external facts, inferences, proposals, and user decisions.

## Mandatory comparator refresh

Open all three official sources on each unblocked fresh selection. Follow relevant official documentation/source links; record publisher, direct URL, access date, and revision or release when available. Verify moved pages and current status instead of relying on these summaries.

| Reference | Research questions | Starting source |
| --- | --- | --- |
| OpenTag | How are channel lifecycle, agent customization, native output, tools, approvals, attachments, and deployment separated? Which guarantees belong to its SDK, application, or managed service? | https://github.com/CopilotKit/OpenTag |
| Claude Tag | How do people delegate shared work, observe progress, control participation, and use scoped connections? What belongs to the channel, administrator, execution environment, or runtime? | https://claude.com/docs/claude-tag/overview |
| Codex in Slack | How does a mention become a task, acquire thread context, select an execution environment, and return status/results? What controls govern identity and result visibility? | https://learn.chatgpt.com/docs/third-party/slack |

These are distinct products. Do not flatten differences in thread subscription, prior-history access, personal versus shared accounts, hosting, or result visibility into a fictional common contract. Compare observable behavior, not undocumented implementation. Do not equate a Slack app with a search connector or assume every reference supports Discord.

For candidate platforms and dependencies, inspect current official API/SDK documentation, releases, permissions, delivery semantics, limits, and test seams. Slack and Discord are starting points from existing plans; additional platforms require a selected scope. Confirm Eino contracts against local committed source and available versions. Competitor designs do not require adopting their backend, AG-UI, cloud service, or packaging.

If a mandatory source cannot be verified, complete local research, label affected claims `unverified-current`, and identify the missing lane. A user may request a repo-only exploratory brief, but it is not a fully researched, plan-ready comparison.
