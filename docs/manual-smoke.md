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
| OS / Go | macOS 26.6.2 / go1.26.8 (darwin/arm64) |
| Date | 2026-09-10 |

Prerequisites: operator-managed test bot installations, an eligible OpenCode
Go key, disposable DM/channel/thread locations and nonsensical prompts. Get
explicit authorization before sending live bot messages to other people.

| Case | Slack | Discord |
| --- | --- | --- |
| Short answer with a nonce | pass | pass |
| Follow-up referring to the nonce | pass | pass |
| Restart, then a reference question | pass | pass |
| Preview edit visible during a long answer | pass | pass |
| `!stop` then continue | pass | pass |
| Long Unicode / code-fence answer, chunk order | pass (escaping and Unicode confirmed visually by the operator) | pass |
| Thread vs DM isolation | pass | pass |
| `!help`, `!new`, denied actor | pass (`!help`, `!new`); denied actor not run | pass (`!help`, `!new`); denied actor not run |
| One platform disconnected, other continues | not run | not applicable (single platform enabled) |
| Effective permissions/intents, no mention notifications | pass | pass |
| Flash accepts no-tools multi-turn history across restart | pass | pass |

Status: both platforms executed on 2026-09-10. Slack ran against an app
created and installed from `slack-manifest.json` with the Slack CLI
(`slack platform run` supplied the bot and app-level tokens to a start hook
that execs `serve`); one DM and two public-channel threads were used, a
600-word answer delivered as two ordered chunks, `!stop` settled the essay
interrupted, `!new` rotated the generation, and the service log recorded no
warnings. The one-platform-disconnect case was not run. Discord half executed on 2026-09-10 against one operator-managed
test installation (DM plus one public thread in one allowed text channel);
every case passed. Observations: the placeholder edited in place while
streaming; a 600-word answer with a Python block delivered as three ordered
chunks within the 1800-unit budget with the fence balanced across the split;
every bot message carried empty allowed mentions and suppressed embeds;
restart preserved the DM conversation; `!stop` settled the run interrupted
with the stopped notice; `!new` rotated the generation and the follow-up ran
on a fresh session; the service log recorded no warnings. Denied-actor was
not run (no second participant available). Failure of the
live eligibility, model or protocol gate blocks release and identifies the
provider owner; it does not authorize a fallback provider or model.
