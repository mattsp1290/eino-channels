# Slack setup

1. Create an app from `slack-manifest.json` at <https://api.slack.com/apps>.
   The manifest enables Socket Mode, the bot scopes `app_mentions:read`,
   `chat:write`, `im:history`, event subscriptions `app_mention` and
   `message.im`, and the App Home messages tab.
2. Generate an app-level token with `connections:write` (`SLACK_APP_TOKEN`).
3. Install the app to one workspace and copy the bot token
   (`SLACK_BOT_TOKEN`).
4. Put the team ID in `slack.team_id`; the service verifies it through
   `auth.test` at startup and refuses to run for another team.
5. Invite the bot to each channel listed in `slack.allowed_channel_ids`.
   Channel history scopes are not needed: follow-ups require a mention and
   the service keeps its own history.

Behavior:

- DMs from allowed users are conversations; no mention required.
- In allowed channels, mention the bot to start a thread. Every follow-up in
  the thread, including `!stop`, `!new`, `!help`, needs a fresh mention.
- Replies are posted in the thread with link and media unfurls disabled,
  `parse=none`, no user or group linking, and `<`, `>`, `&` escaped.
- Slack Connect and external shared channels are ignored.
- Messages are acknowledged only after the prompt is durably stored, so a
  crash between receipt and acknowledgement leads to a Slack retry, not a
  lost prompt. Retries are deduplicated by channel and timestamp.

Not included: `chat:write.public`, user tokens, OAuth or distribution.
