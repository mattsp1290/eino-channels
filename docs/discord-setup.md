# Discord setup

1. Create an application and bot at <https://discord.com/developers>.
   Copy the bot token (`DISCORD_BOT_TOKEN`). Never use a user token.
2. Gateway intents: Guilds, Guild Messages, Direct Messages. The privileged
   Message Content intent is not required: mentions and DMs carry content.
3. Invite the bot to each guild in `discord.guild_ids` with the permissions
   View Channel, Send Messages, Send Messages in Threads, Read Message
   History and Create Public Threads on the channels in
   `discord.allowed_channel_ids`.

Behavior:

- DMs from allowed users are conversations; no mention required.
- Addressing means the bot appears in the message's mentions: an explicit
  `@bot` or a Discord reply to one of its messages both count.
- In an allowed text channel, mentioning the bot creates a public thread
  named "Conversation" from your message; replies go there. Every follow-up
  in the thread, including `!stop`, `!new`, `!help`, needs a mention.
- Private threads, forums, news channels, group DMs and voice are ignored.
- Replies never ping anyone: `allowed_mentions` is empty on every create and
  edit, embeds are suppressed, and `@everyone`/`@here` text is neutralized.
- Each reply create carries a nonce with `enforce_nonce`; an ambiguous create
  is reconciled by scanning the last 100 messages of the destination once.

Limitation: the Gateway has no application-level acknowledgement and no
durable offline backlog. If the service is down or its session cannot be
resumed, prompts sent during that time are not delivered later. Ask users to
resend prompts that got no answer.
