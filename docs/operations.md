# Operations

## Lifecycle

- One process holds an exclusive lock on `state_dir`; a second instance
  exits with a fixed diagnostic.
- Startup opens both databases, then recovers: prompts that were admitting
  or admitted when the process last stopped are resolved through the agent's
  keyed admission receipts. A run abandoned mid-stream is resumed after its
  5 s lease expires and settles as interrupted with no new model call; the
  user sees an interruption notice and can continue.
- Shutdown (SIGINT/SIGTERM) stops ingestion, interrupts owned runs, waits up
  to `limits.shutdown_seconds` for settlement, then closes watches, SDKs and
  pools. On timeout the process exits with code 3 and leaves recovery records
  intact.
- If one platform's adapter fails (for example revoked auth), the other keeps
  serving; the process exits when all enabled adapters have stopped.

## What users see

| Situation | Text |
| --- | --- |
| Model streaming | "Thinking…" then coalesced previews (at most one edit per 1.2 s) |
| Completed | Committed answer, chunked (Slack 3500 characters, Discord 1800 UTF-16 units) |
| `!stop` | Prefix shown so far plus "(Stopped.)" |
| Restart mid-answer | "(This answer was interrupted by a service restart. …)" |
| Provider failure | "Sorry, the model request failed. Please try again." |
| Output cap | Accepted 32 KiB prefix plus "(Output limit reached.)" |
| Queue full | "I am at capacity right now. Please try again in a moment." |
| History limit | "This conversation has reached its history limit. Use !new …" |

Interrupted partial text is shown in chat but is not part of the model's
history: the agent runtime commits only completed answers.

Notices (`!help`, `!new`, denied, capacity, "Stopped.") are transient
best-effort sends through a bounded queue: they can be dropped under load
and are not recovered after a crash. The control itself is recorded
durably before the notice is attempted.

## Delivery guarantees

Admission is exactly-once per platform message. External sends are not:
a create can succeed without a response. Such a delivery is recorded as
`ambiguous_create` and blocks later output on the same conversation until an
operator resolves it. Definite failures are retried with backoff, up to 5 attempts within 10
minutes, then marked `failed`. The model answer is never regenerated to repair
a send.

### Resolving deliveries

Stop the service first; the commands take the exclusive lock.

```
eino-channels delivery list --config config.json
```

Lists unresolved rows: ID, platform, status, operation state, attempts,
chunk index, destination channel and thread (for Discord the channel is the
thread and the last column is the guild). Bodies are never printed.

```
eino-channels delivery resolve --config config.json --id <id> \
  --action associate-message --message-id <remote id> [--confirm-inspected]
```

Use when the message did land. Discord verifies through the API that the
message exists in the stored destination and was authored by the bot. Slack
has no history scope in this deployment: inspect the thread yourself and pass
`--confirm-inspected`; the association is recorded as operator-attested. The
answer is not marked delivered: the latest committed revision is applied as
an edit at the next `serve` start, and only that acknowledged edit unblocks
successors.

```
eino-channels delivery resolve --config config.json --id <id> --action resend
```

Use when the message did not land. The row returns to pending with an audit
marker and is sent at the next `serve` start. A duplicate external message is
possible if the original did land after all.

Neither command calls the model or changes conversations.

## Rollout and rollback

- Start with one test installation, restrictive allowlists and a fresh
  `state_dir`. Run `check-config` before `serve`.
- Back up both databases only while the service is stopped.
- Rollback: stop the process, keep the files, restore the prior binary with
  its matching database backup only under operator direction. Never run an
  old binary against a newer schema; the service refuses and does not delete
  data. A restore can erase recent admission receipts and allow platform
  retries to be admitted again, so keep the installation disconnected until
  the recovery boundary is confirmed.
