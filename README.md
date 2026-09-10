# eino-channels

Slack and Discord text conversations backed by
[eino-agent](https://github.com/mattsp1290/eino-agent) and
[eino-providers](https://github.com/mattsp1290/eino-providers) OpenCode Go
with DeepSeek V4 Flash. One process, one bot installation per platform,
local SQLite state, no tools.

What it does:

- Direct messages and explicitly addressed channel messages start
  conversations. Slack root mentions reply in a thread; Discord root mentions
  create a public thread.
- Multi-turn recall, continuity across restarts, visible progressive output,
  `!stop`, `!new`, `!help`, bounded queues and fixed safe error text.
- Every platform message is admitted at most once through durable keyed
  admission, including across crashes and reconnects.

What it does not do: tools, other models, history imports, private threads,
public distribution, cross-platform shared sessions.

## Commands

```
eino-channels serve --config config.json
eino-channels check-config --config config.json
eino-channels delivery list --config config.json
eino-channels delivery resolve --config config.json --id 12 --action associate-message --message-id <id> [--confirm-inspected]
eino-channels delivery resolve --config config.json --id 12 --action resend
eino-channels version
```

Credentials come only from the environment: `OPENCODE_API_KEY`,
`SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `DISCORD_BOT_TOKEN`.

## Documentation

- [docs/setup.md](docs/setup.md): build, configuration, first run.
- [docs/slack-setup.md](docs/slack-setup.md) and
  [docs/discord-setup.md](docs/discord-setup.md): bot installation.
- [docs/operations.md](docs/operations.md): lifecycle, limits, recovery,
  delivery resolution, rollback.
- [docs/manual-smoke.md](docs/manual-smoke.md): live release gate record.

## Development

```
make check   # gofmt, tidy -diff, verify, no replace, vet, test, race, build
```

Tests never read operator state or real credentials; fake tokens are
explicit and all provider and platform traffic goes to local fixtures.
