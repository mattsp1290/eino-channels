# Slack and Discord conversations

Status: Implemented on branch `conversations` (2026-09-10); credential-free gate passes; live release gate (WP5) pending operator credentials. See 06 for refinements.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "4b94554a9c332658a0f7a6571523289f1a6071aa6d37a9b13aa8d54e8026ce17",
    "confirmed_at": "2026-09-10T03:02:00Z"
  }
}
```

User answered “1. no 2. no”; feature flags are not-applicable. This is a new host, with no legacy API/config/data migration promise. Shared dependency consumers keep their own operating-context decisions; the agent owner separately approved the breaking Start result and database-baseline change. Introduce versioned host storage and fail closed on unknown schemas; never silently delete state to accommodate an upgrade.

## Outcome and success

Create a Go service in this repository that embeds eino-agent and provides text conversations over Slack and Discord, using eino-providers OpenCode Go with DeepSeek V4 Flash and zero tools. “Same quality as eino-tui” means multi-turn recall, durable continuity after relaunch, visible progressive output, explicit interruption, bounded lifecycle and fixed safe errors. It does not promise identical model intelligence or terminal UI features.

Success is a real two-turn exchange on each platform, continuation after service restart, interrupt then follow-up, channel/thread/DM isolation, and deterministic duplicate/crash tests proving one admitted turn per platform message. Final platform text must match committed runtime text after platform escaping/chunking; transient prefixes never become invented completed answers. Verification is specified in WP5.

## Change type and evidence

Greenfield application: command/config, shared conversation service, SQLite host state, agent/provider bridge, two transport adapters, rendering/delivery, integration tests and CI.

- Local repository at `79eb557531ee06a6a577c2adf53dcd7077d053d8` has only `README.md`, `LICENSE`, `.gitignore`; initial worktree clean. No existing Go module, tests, CI, or applicable repository AGENTS.md found.
- Resolve `EINO_AGENT_DIR`, `EINO_TUI_DIR`, `EINO_PROVIDERS_DIR` to matching `github.com/mattsp1290/` checkouts. They are planning source aliases, not runtime environment variables. Inspected agent `3dd3a4391ae75a0d2cc9c19439f391d7d517d0d0`, TUI `f191430afda682cd6c4dac45d96ae9fa00981674`, providers `fb8ed3137d58a87d02b53fc43fddba3ed87e98f6`. Preserve unrelated work in those repositories; this plan changes none of their code.
- `$EINO_TUI_DIR/internal/runtimeui/{wiring,service,pump}.go`, `internal/integration/` and `Makefile` establish durable admission, interrupt/recovery, bounded rendering and credential-free real-runtime tests. TUI pins agent v0.3.2 with the older sqlite.Open API. Do not copy that API into a new host.
- `$EINO_AGENT_DIR/docs/consumer-guide.md`, `watch/service.go`, `runtime/{types,admission,interrupt}.go` establish host-owned SQL pool + sqlite.Migrate/New, bounded snapshot/live observation and no-tools recovery as interrupted. Its prior SQL/watch publication was `v0.3.4-0.20260910012408-cec27e5eb734`. Adopt `v0.3.4-0.20260910151824-b77c7e64e09d`, which adds keyed admission and preserves host-pool/watch APIs; Start now returns AdmissionResult and old database baselines are rejected.
- `$EINO_PROVIDERS_DIR/opencodego/{config,chatmodel,adapters,transport,stream}.go` already provides NewChatModel and ProtocolChatCompletions. Public download of `v0.0.0-20260910030140-fb8ed3137d58` resolved to the inspected full SHA with checksums, without local replacement. This verifies availability, not live model compatibility or combined build success.
- Existing provider request is still marked open despite shipped source; do not require its unrelated three-protocol/tool roadmap to finish for this host.

## Decisions and target flow

Use one process, one bot installation per enabled platform, local SQLite, Slack Socket Mode and Discord Gateway. Platforms can run independently or together. Host adapters own authorization, routing and delivery; eino-agent owns transcript, runs, recovery and provider history; eino-providers owns OpenCode wire/auth adaptation. Keep host packages internal. No new shared repository or extraction of TUI internals is justified.

Platform event → validate/filter/authorize → durable host inbox → serialized conversation worker → keyed agent admission → provider stream → watch projection → coalesced platform edits → durable final delivery record.

Use DMs and explicitly addressed channel/thread messages. Slack root mentions start a reply thread. Discord root mentions in allowed text channels create a public thread; subsequent thread prompts require a bot mention. Thread conversations are shared by authorized participants; DMs stay private. No background channel-history ingestion. See WP3/WP4 for exact keys and permissions.

The sole startup selection is `opencode-go` / `deepseek-v4-flash`, explicit Chat Completions. No model picker, fallback, tool mounts, shell/workspace access, attachment parsing, web fetch, OAuth UI, public distribution, cross-platform bridging, deployment provisioning, autonomous actions, renaming/search or historical backfill. Host commands for stop/new/help are transport controls, not model tools.

## Requests and readiness

| Canonical location | Owner / consumers | Effect | Exact unblock |
| --- | --- | --- | --- |
| `$HOME/.agents/projects/eino-agent/requests/2026-09-09-idempotent-channel-admission.md` | Matt / agent maintainer; channels is the requesting consumer, other ingress hosts prospective | Completed upstream; WP0 local integration gate remains | Same-filename response records PR #33, merge CI, published `v0.3.4-0.20260910151824-b77c7e64e09d` and actual APIs mapped in WP2 |
| `$HOME/.agents/projects/eino-providers/requests/2026-09-08-opencode-go-provider.md` | Matt / provider maintainer; auth-library origin, channels now prospective adopter | Informational existing request; broad completion not required | Existing Chat path is downloadable; WP0 clean consumer and WP5 live Flash tests establish this host's narrower adoption |

No request is accepted merely because it was written. The verified completion response at `$HOME/.agents/projects/eino-agent/responses/2026-09-09-idempotent-channel-admission.md` supplies the contract. A host-only in-memory dedup map or a second transcript cannot close the crash window between runtime admission and host receipt persistence. Changing that guarantee requires an explicit plan revision.

## Assumptions, risks and open questions

- Blocking local verification: implementing agent must pass WP0 combined module/SDK checks and record selected SDK pins before WP1. Upstream admission is implemented and published; the actual contract is mapped in WP2 and the completion response.
- Non-blocking product defaults: single operator deployment; finite explicit allowlists; no historical backfill; mention required for thread follow-ups. These are implementation decisions, not user-confirmed operating context.
- Launch gate: operator supplies test bot installations, tokens, allowed IDs and eligible OpenCode access. No secrets were inspected or live messages/inference sent during planning. [OpenCode documents](https://opencode.ai/docs/go/) coding-agent-oriented traffic and the deepseek-v4-flash Chat Completions route; use truthful `eino-channels/<version>` identity and check service suitability for this deployment without disguising traffic or silently changing providers.
- Delivery is not globally exactly-once: remote create may succeed without a response. Preserve ambiguous delivery state, never regenerate the model answer, and give a bounded reconciliation/retry procedure. Discord has no durable offline event backlog guarantee; disclose possible missed prompts during non-resumable downtime.
- Finite history and output limits are explicit in WP2; they protect a long-running service and offer a new conversation rather than silently trimming context.

## Document map

1. [01-dependencies-and-bootstrap.md](01-dependencies-and-bootstrap.md): WP0 dependency gate and WP1 application/state foundation.
2. [02-conversation-runtime.md](02-conversation-runtime.md): WP2 routing, admission, provider, projection and recovery.
3. [03-slack.md](03-slack.md): WP3 Slack ingress and delivery.
4. [04-discord.md](04-discord.md): WP4 Discord ingress and delivery.
5. [05-verification.md](05-verification.md): WP5 integrated quality and live acceptance.
6. [06-execution-handoff.md](06-execution-handoff.md): ordered execution, gates, rollback and done.
