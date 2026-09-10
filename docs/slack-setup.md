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

## Creating the app with the Slack CLI

The manifest can be applied without the web UI. In a scratch directory
outside the repository:

```
mkdir -p .slack && cp /path/to/slack-manifest.json manifest.json
cat > .slack/hooks.json <<'HOOKS'
{"hooks": {"get-manifest": "./get-manifest.sh", "start": "./start.sh"},
 "config": {"manifest": {"source": "local"}, "sdk-managed-connection-enabled": true}}
HOOKS
printf '#!/bin/sh\ncat "$(dirname "$0")/manifest.json"\n' > get-manifest.sh
cat > start.sh <<'START'
#!/bin/sh
# The CLI supplies SLACK_BOT_TOKEN and SLACK_APP_TOKEN. Export the rest
# from your own environment. Never echo these variables and never use set -x.
cd /path/to/eino-channels || exit 1
exec ./eino-channels serve --config config.json
START
chmod +x get-manifest.sh start.sh
slack manifest validate
slack app install --team <TEAM_ID> --environment local
slack platform run --team <TEAM_ID>
```

`slack platform run` starts `start.sh` with `SLACK_BOT_TOKEN` and
`SLACK_APP_TOKEN` in its environment, so the service itself never writes
the tokens to disk. The Slack CLI keeps its own login under `~/.slack/`;
treat that directory as sensitive.
