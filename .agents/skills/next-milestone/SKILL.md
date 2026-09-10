---
name: next-milestone
description: Choose the next eino-channels milestone toward bringing eino-agent into shared conversations, using live repository and ecosystem evidence plus current OpenTag, Claude Tag, and Codex in Slack comparisons. Use when asked what this repository should build next or with $implementation-plan $next-milestone; not for implementing an already selected plan.
---

# Next Eino Channels Milestone

Choose one bounded increment toward making eino-channels serve eino-agent as OpenTag, Claude Tag, and Codex in Slack serve their respective ecosystems: people bring an agent into a conversation, give it context and work, follow its progress, and receive useful results where the conversation happened.

This product direction includes collaborative agent work, tools, approvals, artifacts, and operator experience as possible future capabilities. A text conversation can be a foundation, but is not the entire destination. Comparator platforms, deployment models, and features are research evidence, not automatic requirements or permission to expand a selected milestone.

## Ownership and ordering

For `$implementation-plan $next-milestone`, resolve the repository and guidance first, then run this skill before naming or creating a plan. This skill owns request gating, research, candidates, selection, and the resolved brief. `$implementation-plan` owns its normal operating-context decisions, plan files, and reviews. Preserve already applicable user answers and authorization.

Standalone use returns a brief in conversation. Do not create application code, an interim roadmap, tracker issues, or a second planning format. Local upstream request records are the only additional artifacts this workflow may write, within the authorization described in the request protocol. Do not execute this workflow merely because you are editing the skill.

## Workflow

1. **Check existing blockers.** Read [upstream-request-protocol.md](references/upstream-request-protocol.md). Inspect outbound requests for the exact consumer `eino-channels`, including legacy blockers explicitly linked by active plans. Verify responses against committed public code and usable pins. Report all unresolved blockers and stop before fresh selection unless the user has withdrawn or superseded the affected milestone. Incoming requests are demand evidence, not outbound blockers.
2. **Rebuild the frontier.** Read [research-playbook.md](references/research-playbook.md). Inspect this repository, its plans, and relevant local Eino repositories without modifying them. Refresh all three official comparator sources on each fresh selection. Distinguish implemented behavior from planned work and unknowns.
3. **Compare bounded outcomes.** Read [frontier-rubric.md](references/frontier-rubric.md). Build the capability matrix and current/target conversation flow. Present 2–4 distinct candidates, recommendation first, each with evidence, ownership, readiness, trade-offs, and acceptance. Show fully planned work separately rather than proposing duplicates. If fewer defensible candidates exist, explain the constraint instead of inventing options.
4. **Resolve selection.** Ask the user to select after displaying options, unless they have already selected a still-valid milestone or explicitly delegated the choice. Preserve their selection. Ask up to three concise questions per interaction only for unresolved decisions that materially change behavior, security, ownership, persistence, or scope.
5. **Resolve dependencies.** Trace each required public contract to its verified owner. Follow the request protocol for missing sibling contracts. An unresolved upstream dependency blocks planning; a speculative local adapter or copied runtime is not a substitute.
6. **Hand off.** Read [implementation-plan-handoff.md](references/implementation-plan-handoff.md). Build the brief in conversation. If ready and composed with `$implementation-plan`, incorporate it into exactly one normal plan before that skill's reviews. If standalone, deliver the brief and its planning invocation. If a material decision remains unanswered, retain the selected milestone in a blocked brief with its owner and exact unblock action.

## Invariants

- Verify current ownership: eino-channels normally owns platform ingress, identities, authorization policy, routing, context selection, native interaction, delivery, installation, and operations; eino-agent owns runtime execution and authoritative session state. Providers, tools, extensions, protocols, and observability remain with their established owners.
- Use public Eino APIs and consumable immutable versions. Sibling working changes and local replacement directives do not prove a shipped dependency.
- A usable agent journey must traverse platform ingress → authorized conversation/context → public eino-agent admission/execution → ordered observed progress → platform result, with cancellation and failure behavior. A scripted model can test that real path without proving live-provider or live-platform success.
- Separate transport deduplication, durable runtime admission, and external delivery. Never infer globally exactly-once effects from one of them. Account for ambiguous sends, replay, reconnect, rate limits, and bounded queues when affected.
- Treat shared-thread membership, acting identity, connected-account scope, approval authority, private results, and bot loops as explicit design boundaries. Chat content is untrusted input, not administrator policy.
- Do not infer permission to send live messages, install apps, deploy services, invoke paid models, or change sibling code from a selection/planning request.
- Never inspect secret-bearing configuration values or private conversations for research, or put credentials, raw reasoning, or sensitive payloads into briefs, logs, or requests.
