# Execution handoff

Status (2026-09-10): WP0–WP5 implemented on branch `conversations` (commit a06ab95 and follow-ups). Credential-free gate `make check` passes locally; four dual-reviewer passes ended in approval at commit c5ceb53. The live release gate in [05-verification.md](05-verification.md) passed on both platforms on 2026-09-10 (recorded in `docs/manual-smoke.md`); not run: the denied-actor case (no second participant) and the one-platform-disconnect case.

Implementation notes that differ from or refine the plan text:

- Interrupted answers: the pinned eino-agent runtime commits no partial assistant text on interruption (only completed answers are persisted). The host therefore shows the in-process live prefix labeled "(Stopped.)" for a user stop; after a restart only the interruption notice is delivered. Interrupted partial text is not in the model's history. Documented in `docs/operations.md`.
- Output cap: when the model completes before the 32 KiB streaming cap could cancel it, the accepted prefix is delivered with "(Output limit reached.)" and result code `output_limit`.
- Discord nonce: DiscordGo v0.29.0 has no nonce field on MessageSend; creates go through `Session.RequestWithBucketID` with a raw JSON body carrying `nonce` and `enforce_nonce`. Reconcile decodes the raw message list to read `nonce`.
- Discord 502 on create is classified ambiguous (DiscordGo returns a plain error, not a RESTError, when its retries are disabled).
- Delivery spacing/preview coalescing is a service option (default 1.2 s) so fixtures can shorten it.
- Notices (help, denied, capacity, stopped) are transient best-effort sends through a bounded queue, not durable deliveries.
- Adapters ship as one `adapter.go` per platform (plus `render.go` for Slack) rather than the `{adapter,events,delivery,threads}.go` split proposed in 03/04.
- `internal/integration/isolation_test.go` does not exist; isolation coverage lives in `conversation_test.go` (`TestRealProviderTwoTurnsReopenAndIsolation`).
- The Slack persist-before-ack deadline defaults to 1 s (the plan's target), configurable through the adapter options.
- eino-providers has a newer pseudo-version (c81e1b0110d7) on the proxy; the module pins the WP0-verified fb8ed3137d58 and `make check-mod` enforces it.

## Ordered packages

| Order | Result and files | Prerequisite | Verification |
| --- | --- | --- | --- |
| WP0 | Verify mapped admission contract with the combined agent/provider/SDK graph and record SDK pins | Completed response and published agent pin already recorded | clean external consumer; no Replace; real receipt/reopen/watch fixture |
| WP1 | Go binary/config, protected databases, inbox/delivery schema, build/CI skeleton; new files listed in 01 | WP0 | config/state/app tests, race, build, lock/schema checks |
| WP2 | Agent/provider bridge, serialized routing, controls, bounded rendering/recovery; files in 02 | WP1 | real runtime/provider fixture, crash/idempotency/isolation/limits |
| WP3 | Slack Socket Mode and setup; files in 03 | WP2 interfaces | real SDK fake HTTP/envelope tests, ack/destination/redaction |
| WP4 | Discord Gateway and setup; files in 04 | WP2 interfaces | real SDK fake HTTP/Gateway tests, thread/reconnect/rate limits |
| WP5 | Cross-platform integration, delivery recovery CLI, completed docs and release evidence; files in 05 | WP3 + WP4 | make check and authorized live test matrix |

WP3 and WP4 may be separate worktrees/PRs after WP2; keep shared interface edits with WP2 owner. All other packages are sequential. WP0 may create its disposable verification module outside checkouts. Do not create the application module or implement a local workaround before WP0 passes. Use the repository's issue tracker for execution once implementation is requested; this directory is a specification, not execution-status tracking.

## Completion and handoff

Definition of done: no tools reach the wire or runtime; DeepSeek Flash through eino-providers OpenCode Go completes multi-turn conversations on both platforms; restart preserves context; duplicate events never admit a second turn; stop/new/queue/error behaviors are visible and bounded; destinations and credentials stay isolated; final output agrees with committed transcript; SDK/network ambiguity is honestly surfaced and recoverable; credential-free gate and live release matrix pass on recorded public pins.

No model-quality equivalence claim to Codex/TUI is made. External create is not exactly-once. Discord offline missed events and Slack manual ambiguous-send resolution remain documented v1 limitations. Deferred: tools, other models/providers, history imports, private/forum Discord threads, unsolicited participation, public distribution/multi-installation tenancy, cross-platform shared sessions, horizontal scaling, compaction/search/renaming and automated deployment.

Before implementation, re-read repository instructions/current dirty state and preserve unrelated changes. After implementation, hand off source version, dependency pins, check results, operator setup, known delivery limitations and next release gate. Do not claim cross-repository requests are accepted or completed without the response and tested pin.
