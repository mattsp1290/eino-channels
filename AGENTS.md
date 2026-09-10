# AGENTS.md

Guidance for coding agents working in `github.com/mattsp1290/eino-channels`.
Read this before editing; it is the source of truth for repository
conventions. Shared agent material (plans, skills) lives under `.agents/`.

## What this repository is

A single-process Go service that gives Slack and Discord text conversations
with an agent backed by `eino-agent` and the OpenCode Go provider from
`eino-providers` (DeepSeek V4 Flash, Chat Completions, no tools). State is two
SQLite databases in an exclusively locked directory. The design and
acceptance criteria are in `.agents/plans/slack-discord-conversations/`
(read `00-overview.md` and `06-execution-handoff.md` first).

## Commands

```
make check        # gofmt, go mod tidy -diff, verify, exact pins, vet, test, race, build
make test         # go test ./...
make test-race    # go test -race ./... (100-turn tests skip under race)
make build        # ./eino-channels
```

Always run Go with `GOWORK=off GOTOOLCHAIN=auto`; the Makefile exports both.
`make check` is the merge gate and CI runs exactly it on Linux.

## Layout

| Path | Owns |
| --- | --- |
| `cmd/eino-channels` | CLI entry point only |
| `internal/app` | assembly, `serve`, `check-config`, `delivery list/resolve` |
| `internal/config` | config file parsing/validation, env credentials, fixed provider constants |
| `internal/state` | host SQLite: lock, protected files, versioned schema, inbox, conversations, deliveries |
| `internal/agentbridge` | eino-agent embedding: registry, watch, orchestrator, fixed resolver, stream wrapper |
| `internal/conversation` | transport-neutral service: ingest, scheduler, turns, controls, delivery lane, recovery |
| `internal/render` | escaping, mention suppression, Unicode-safe chunking, fence balancing |
| `internal/slack`, `internal/discord` | platform adapters: ingress validation and delivery |
| `internal/testkit` | scripted provider, recording deliverer, assembled service for tests |
| `internal/integration` | real provider wire tests against a fake OpenCode server |
| `docs/` | setup, platform setup, operations, live smoke record |

No Slack or Discord SDK type may cross into `internal/conversation`; adapters
talk to the service through `state.Inbound` and `conversation.Deliverer`.

## Invariants that must not regress

- One admission per platform message: durable inbox row first, then keyed
  `runtime.Start` with `evt-<sha256>`; after any crash look up the receipt
  before retrying. Never manufacture execution from a receipt.
- Slack envelopes are acknowledged only after the inbox write commits.
- Delivery rows keep `remote_id` once a create returned one; no path creates
  twice for a row. Ambiguous and failed rows block their route's lane until
  an operator resolves them; the model answer is never regenerated for a send.
- Final text comes only from the committed transcript. Previews are transient.
- The `unresolvedDelivery`/`schedulableDelivery` predicates in
  `internal/state/deliveries.go` define "needs work" for every query;
  `Delivery.Resolved()` mirrors them for callers and `TestPredicatesAgree`
  keeps the three in step.
- Credentials come only from the environment and never reach logs, chat,
  config snapshots, or error text. Tests plant sentinel strings to prove it.
- Provider and model are constants (`opencode-go`, `deepseek-v4-flash`);
  `request.Tools` must be empty on the wire.
- Dependency pins are enforced by `make check-mod`; do not let `go mod tidy`
  drift `eino-providers` to a newer pseudo-version.

## Upstream facts worth knowing

- `eino-agent` commits no partial assistant text on interruption; the host
  renders the live prefix labeled "(Stopped.)" and it is not in history.
- The eino-ext OpenAI adapter sets `Message.Extra`; the bridge strips it or
  the runtime rejects the run.
- DiscordGo v0.29.0 has no nonce field on `MessageSend`; creates use a raw
  request body with `nonce` and `enforce_nonce`. Its 502 path returns a plain
  error, classified ambiguous on create by design.

## Workflow

- Plan → dual review → implement → fix → verify → commit. Reviews are
  written under `reviews/` (gitignored) by the `review` skill; do not delete
  earlier review directories.
- Commit finished, verified units; never force-push or push to `main`.
- Tests must stay credential-free and must not read operator state; use
  `internal/testkit` and fake HTTP servers.
- Keep `docs/manual-smoke.md` free of tokens, account IDs, private channel
  IDs, prompt or answer bodies.

## Local operation

- `config.json` at the repo root and `state/` are gitignored; operator
  credentials live in the shell environment (`OPENCODE_API_KEY`,
  `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `DISCORD_BOT_TOKEN`).
- The Slack app can be created from `slack-manifest.json` with the Slack CLI;
  see `docs/slack-setup.md` for the `slack platform run` start-hook pattern
  that injects the two Slack tokens into `serve` without writing them to disk.
- Operator delivery resolution requires the service stopped; see
  `docs/operations.md`.

## Known limitations

External sends are not exactly-once; Discord has no offline event backlog;
Slack ambiguous creates need manual resolution; the denied-actor and
one-platform-disconnect live cases have not been exercised.
