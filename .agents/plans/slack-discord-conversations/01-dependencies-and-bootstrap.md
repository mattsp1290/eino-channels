# WP0: dependency readiness; WP1: host foundation

## WP0 — resolve contracts before implementation

Prerequisite: overview context confirmed; upstream admission complete at `v0.3.4-0.20260910151824-b77c7e64e09d`. Owner: implementing agent. Actual APIs and fingerprint semantics are mapped in WP2 and the canonical completion response. Read published source at full commit `b77c7e64e09d6c7aeb49b63ef3bca230588dc852`, not a stale local checkout. The remaining blocker is local combined dependency/SDK verification, not upstream acceptance or publication. These checks have not yet been run for eino-channels.

In a disposable consumer outside all checkouts, set GOWORK=off and GOTOOLCHAIN=auto, initialize a temporary module, and go get `github.com/mattsp1290/eino-agent@v0.3.4-0.20260910151824-b77c7e64e09d` plus `github.com/mattsp1290/eino-providers@v0.0.0-20260910030140-fb8ed3137d58`. Compile a tiny public-API fixture that opens/migrates a host-owned SQLite pool, makes a watch, constructs the no-tools registry, builds OpenCode ChatCompletions against a fake transport, submits a keyed turn, looks up its receipt, and reopens state. Run go test, go mod verify and `go list -m -json all`; reject every Replace field. Record resolved Go/Eino/auth dependencies and exact versions in the plan before WP1. No paid request is needed. Eino baseline is v0.8.13; Go target is 1.26.8 matching TUI. Resolve any incompatible module upgrade explicitly in this gate.

Select immutable SDK releases for `github.com/slack-go/slack` and `github.com/bwmarrin/discordgo`, with constructor, context-aware send/edit, socket/gateway shutdown, reconnect and fake transport seams compiled in the same consumer. Candidate Discord release is v0.29.0; choose a verified Slack release at implementation time and record both in go.mod and setup docs. [Slack's SDK example](https://github.com/slack-go/slack/blob/master/examples/socketmode/socketmode.go) and [DiscordGo](https://github.com/bwmarrin/discordgo) establish the intended libraries. This is a bounded dependency-resolution step, not permission to substitute platforms or provider.

### WP0 result (2026-09-10)

Passed in a disposable consumer outside all checkouts (GOWORK=off, GOTOOLCHAIN=auto, Go 1.26.8, `go test -race`, `go mod verify`, zero Replace modules). Recorded pins:

| Module | Version |
| --- | --- |
| github.com/mattsp1290/eino-agent | v0.3.4-0.20260910151824-b77c7e64e09d |
| github.com/mattsp1290/eino-providers | v0.0.0-20260910030140-fb8ed3137d58 |
| github.com/mattsp1290/opencode-auth-go | v0.0.0-20260908211055-a3f44cca7a18 |
| github.com/cloudwego/eino | v0.8.13 |
| github.com/cloudwego/eino-ext/components/model/openai | v0.1.13 |
| modernc.org/sqlite | v1.54.0 |
| github.com/slack-go/slack | v0.29.0 |
| github.com/bwmarrin/discordgo | v0.29.0 |

Findings that shape WP2–WP4:

- `Start` returns `runtime.AdmissionResult`; duplicates return `AdmissionExisting` with nil Handle and the same receipt; changed payload returns `session.ErrAdmissionConflict`; root `session.Store.LookupAdmission` returns `session.ErrNotFound` for absent keys.
- The eino-ext OpenAI adapter populates `schema.Message.Extra` on assistant deltas; the runtime rejects that without a provider-state codec (`provider_state_unregistered`). The bridge streamer wrapper must clear `Extra` on every delta and fail the turn on any tool call.
- Per-call provider session identity works through `opencodeauth.WithSessionID(ctx, id)`; the wire carried `x-opencode-session`, truthful `User-Agent`, `Bearer` auth, path `/zen/go/v1/chat/completions`, model `deepseek-v4-flash`, no `tools`/`tool_choice`.
- Runtime IDs must be collision-resistant across process restarts: `AdmitRun` rejects any existing run ID with `session.ErrConflict`.
- The runtime loads the whole session history before each turn (paged `ListMessages`, no cap); the host must bound history itself before admission.
- DiscordGo v0.29.0 `MessageSend` has no nonce field; `Session.RequestWithBucketID` with a raw JSON body carries `nonce` and `enforce_nonce` on the wire. Default REST retry (502) and 429 auto-retry must be disabled (`MaxRestRetries=0`, `ShouldRetryOnRateLimit=false`) so the host owns create ambiguity.
- slack-go v0.29.0 does not retry unless `OptionRetry` is set; 429 surfaces as `*slack.RateLimitedError` with `RetryAfter`.

Acceptance: clean consumer compiles/runs using actual APIs, no code imports TUI internal packages, keyed admission's crash test and watch/SQLite integration pass. If unmet, keep Status: Blocked with exact owner/action and do not start dependent work.

## WP1 — proposed change surface

All following directories, files and host symbols are **new/proposed**, rooted at existing repository `.`. New directories are created by this package; later work anchors inside them.

- `go.mod`, `go.sum`: module `github.com/mattsp1290/eino-channels`, Go 1.26.8 and WP0 pins.
- `cmd/eino-channels/main.go`, `internal/app/app.go`, `internal/config/config.go`: parse `serve --config <path>` and `check-config --config <path>`, validate, assemble and shut down. check-config performs no network or inference and never prints secrets.
- `internal/state/{store,migrate,inbox,conversations,delivery}.go`: application database and versioned schema.
- New empty parent packages `internal/conversation/`, `internal/agentbridge/`, `internal/render/`, `internal/slack/`, `internal/discord/`, `internal/integration/` are the insertion points for WP2–WP5.
- `config.example.json`, `Makefile`, `.github/workflows/ci.yml`, `docs/setup.md`, `docs/operations.md`; revise existing `README.md` and `.gitignore` for commands, local state/binaries and agent planning artifacts. Do not commit plan artifacts by force.

Proposed configuration: state_dir absolute path; slack.enabled, slack.team_id, slack.allowed_channel_ids, slack.allowed_user_ids; discord.enabled, discord.guild_ids, discord.allowed_channel_ids, discord.allowed_user_ids. Nonempty user allowlist required for each enabled adapter including DMs, and channel/guild allowlists for shared surfaces. At least one platform enabled. Credentials only from `OPENCODE_API_KEY`, `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `DISCORD_BOT_TOKEN` as needed; host passes provider key explicitly, independent of helper env fallback. Fixed provider/model/protocol are constants, not unrestricted URL config. System prompt is an application-owned concise conversation prompt, no tool claims. No tokens in config snapshots or metadata.

Defaults: four concurrent running conversations globally; at most one active + eight queued prompts per conversation; 256 pending inbox items globally; 16 KiB normalized UTF-8 prompt limit; 120s model turn deadline; 10s platform API call deadline; 10s graceful shutdown deadline; 5s agent lease. Validate any exposed limit overrides for positivity and hard ceilings; keep unneeded tuning internal for v1. Reserve controls so a full prompt queue cannot block stop/new/help.

Use a single-process exclusive state-directory lock before opening either database; reject second instance. Protect directory 0700 and databases/lock 0600, reject symlink targets; include SQLite WAL/SHM files in protected directory. Agent database `sessions.db` uses sqlite.Migrate/New with a host-owned database/sql pool, foreign_keys enabled and busy_timeout. Host `channels.db` owns only routing, inbox and delivery; never change agent tables. Version host schema, migrate transactionally, and fail without data deletion on unknown/newer versions. Existing agent schema changes follow WP0's explicit migration/recreation contract. Close both pools only after workers/watch finish.

Proposed host tables (internal schema, new):

- conversations: unique canonical platform route key + generation, opaque runtime session ID, opaque stable provider session ID, route identifiers, creator user ID, creation time. Raw platform IDs never serve as provider session headers. Persist the route before execution.
- inbox: unique installation + platform message ID (Slack team/channel/ts canonical identity also unifies app_mention/message delivery; Discord bot/channel/message ID), normalized prompt/control, authorized actor, conversation generation, frozen non-secret execution settings, content hash, FIFO sequence, state, admission key, receipt/run ID, safe result code; stop controls additionally freeze target run/admitting key and queue sequence cutoff. Pending input is host ingress data, not authoritative transcript; clear prompt/config payload after confirmed receipt; retain dedup tombstone and receipt reference.
- deliveries: unique run/control + chunk index, destination, generation, route delivery sequence, external message ID, deterministic content hash, desired final revision and acknowledged content revision, desired final text while pending, safe status, retry_at, operation state including ambiguous_create. Persist plans before send and responses after send. Clear final payload after acknowledged delivery; committed agent text remains authoritative.

Admission, queue capacity reservation and routing selection commit in a host transaction. Never acknowledge a runnable Slack event before that transaction. Keep dedup tombstones for the deployment lifetime in v1; no TTL silently allows old events back in. Disk-full or schema failure stops new admission and reports a fixed local diagnostic. No unbounded in-memory goroutines per conversation: finite workers and idle cache eviction while durable rows remain.

Acceptance/tests: table-driven invalid config and redaction tests; real SQLite uniqueness/rollback/schema mismatch/reopen; two-process lock test; permission/symlink rejection; queue-capacity atomic races. Run `go test ./internal/config ./internal/state ./internal/app`, race versions, and `go build ./cmd/eino-channels`. Bootstrap Makefile check aggregates formatting, tidy -diff, module verify/no-replace, vet, tests, race and build; CI runs it without credentials on Linux.
