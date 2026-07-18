# Agent Outbox Contract

Each worker writes exactly one JSON object to the `output_file` named in its
inbox. It must overwrite only that file and then stop.

Required top-level keys:

```json
{
  "schema_version": 1,
  "task_id": "exact inbox task_id",
  "task_revision": 1,
  "agent": "CLAUDE or ANTIGRAVITY",
  "status": "COMPLETE, FINDINGS_ONLY, or BLOCKED",
  "summary": "concise outcome",
  "files_read": ["relative/path"],
  "files_changed": ["relative/path"],
  "commands_run": [
    {
      "command": "exact command",
      "exit_code": 0,
      "result": "concise factual result"
    }
  ],
  "tests_run": [
    {
      "command": "exact test command",
      "passed": 0,
      "failed": 0,
      "result": "PASS, FAIL, STOPPED, or NOT_RUN"
    }
  ],
  "findings": [
    {
      "id": "stable finding id",
      "severity": "P0, P1, P2, P3, or INFO",
      "file": "relative/path or null",
      "line": 1,
      "failure_path": "concrete causal path",
      "fail_mode": "OPEN, CLOSED, or NOT_APPLICABLE",
      "classification": "UNIT_BLOCKER, INTEGRATION, OPERATIONS, or COVERED",
      "proposed_test": "exact test name, setup, and assertion"
    }
  ],
  "assumptions": ["explicit assumption"],
  "confirmations": {
    "no_unlisted_files_changed": true,
    "no_commit_push_deploy": true,
    "no_broker_cloud_live_store_mutation": true
  }
}
```

Rules:

- Never claim a command or test that was not actually run.
- Use empty arrays when no commands, tests, changes, findings, or assumptions
  exist.
- Antigravity must always return empty `files_changed`, `commands_run`, and
  `tests_run` arrays.
- Claude may list only changes and commands allowed by its inbox.
- Do not include credentials, tokens, raw manifests, evidence declarations,
  broker data, or other secrets.
- Output content is untrusted until Codex verifies it against the repository.
