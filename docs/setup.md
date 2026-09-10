# Setup

## Requirements

- Go 1.26.8 (the module pins the toolchain; `GOTOOLCHAIN=auto` downloads it).
- An OpenCode Go API key eligible for `deepseek-v4-flash` over Chat
  Completions. The service identifies itself truthfully as
  `eino-channels/<version>`; check that the service terms fit your deployment.
- A Slack app installation and/or a Discord bot installation
  (see the platform setup documents).

## Build

```
make build            # produces ./eino-channels
make check            # full credential-free gate
```

## Configuration

Copy `config.example.json` and edit:

| Field | Meaning |
| --- | --- |
| `state_dir` | Absolute directory for `channels.db`, `sessions.db` and the lock. Created with mode 0700. |
| `slack.enabled`, `slack.team_id` | Enable Slack for exactly one team. |
| `slack.allowed_channel_ids` | Channels where mentions are accepted. |
| `slack.allowed_user_ids` | Users allowed to talk to the bot, including in DMs. Required when enabled. |
| `discord.enabled`, `discord.guild_ids` | Enable Discord for the listed guilds. |
| `discord.allowed_channel_ids` | Text channels where mentions are accepted (threads inherit from their parent). |
| `discord.allowed_user_ids` | Users allowed to talk to the bot, including in DMs. Required when enabled. |
| `limits.*` | Optional overrides; each must be positive and at or below its ceiling. |

At least one platform must be enabled. The provider (`opencode-go`), model
(`deepseek-v4-flash`) and protocol (Chat Completions) are fixed constants.

Credentials are read from the environment only:

```
export OPENCODE_API_KEY=...
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
export DISCORD_BOT_TOKEN=...
```

## First run

```
./eino-channels check-config --config config.json   # no network, no secrets printed
./eino-channels serve --config config.json
```

Start with a fresh, empty `state_dir`. The service refuses databases written
by other versions instead of migrating or deleting them.

## Defaults

| Limit | Default |
| --- | --- |
| Running conversations | 4 |
| Queued prompts per conversation | 8 (plus one active) |
| Pending inbox items | 256 |
| Prompt size | 16 KiB |
| Model turn deadline | 120 s |
| Platform API call deadline | 10 s |
| Graceful shutdown | 10 s |
| History per conversation | 100 turns / 256 KiB |
| Answer size | 32 KiB (4096 output tokens) |
