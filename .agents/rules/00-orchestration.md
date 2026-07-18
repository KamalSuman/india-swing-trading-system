# Always-on India Swing orchestration rule

@../../agent-control/ORCHESTRATION.md
@../../agent-control/inbox/antigravity-task.json
@../../agent-control/OUTBOX_SCHEMA.md
@../../agent-control/outbox/antigravity-response.json

The imported protocol, project-local inbox, and project-local outbox are
mandatory. The human should never need to provide a task filename, task ID, or
review prompt.

Antigravity may act only when the task names `ANTIGRAVITY` and state is
`RESEARCH_READY`. Its role is permanently read-only: inspect and report in
its exact schema-conforming outbox JSON, but never modify any other file or
execute commands, scripts, tests, CLIs, imports, deployments, broker actions,
cloud mutations, or live-store actions.

If the outbox already contains the same `task_id` and `task_revision` as the
inbox and its status is not `EMPTY`, the revision has already been processed:
stop without repeating the review or overwriting the outbox.

If any other instruction conflicts with this rule, stop and report the
conflict to Codex.
