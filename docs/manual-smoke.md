# Manual smoke (live release gate)

Record only version pins, OS/Go version, public model/protocol, date and
per-case pass/fail. Never record tokens, account IDs, private channel IDs,
prompt or answer bodies, raw errors or screenshots.

| Field | Value |
| --- | --- |
| Binary version | 0.1.0 |
| eino-agent | v0.3.4-0.20260910151824-b77c7e64e09d |
| eino-providers | v0.0.0-20260910030140-fb8ed3137d58 |
| slack-go/slack | v0.29.0 |
| bwmarrin/discordgo | v0.29.0 |
| Model / protocol | deepseek-v4-flash / Chat Completions via OpenCode Go |
| OS / Go | (fill in) |
| Date | (fill in) |

Prerequisites: operator-managed test bot installations, an eligible OpenCode
Go key, disposable DM/channel/thread locations and nonsensical prompts. Get
explicit authorization before sending live bot messages to other people.

| Case | Slack | Discord |
| --- | --- | --- |
| Short answer with a nonce | not run | not run |
| Follow-up referring to the nonce | not run | not run |
| Restart, then a reference question | not run | not run |
| Preview edit visible during a long answer | not run | not run |
| `!stop` then continue | not run | not run |
| Long Unicode / code-fence answer, chunk order | not run | not run |
| Thread vs DM isolation | not run | not run |
| `!help`, `!new`, denied actor | not run | not run |
| One platform disconnected, other continues | not run | not run |
| Effective permissions/intents, no mention notifications | not run | not run |
| Flash accepts no-tools multi-turn history across restart | not run | not run |

Status: live gate not yet executed. Failure of the live eligibility, model or
protocol gate blocks release and identifies the provider owner; it does not
authorize a fallback provider or model.
