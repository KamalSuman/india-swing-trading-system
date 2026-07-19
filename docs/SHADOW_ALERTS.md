# Research-only shadow alerts

The shadow-alert layer converts one integrity-verified, complete
`PipelineResult` into a paper-observation message. It is a product-facing
inspection and notification handoff, not a path around the promotion gate.

Every shadow alert:

- is permanently marked `RESEARCH_ONLY` and starts with
  `RESEARCH-ONLY SHADOW ALERT — DO NOT EXECUTE`;
- requires the source pipeline result and nested trade decision to pass their
  content-integrity checks;
- rejects failed pipeline runs and any decision marked `execution_eligible`;
- binds the exact pipeline run, snapshot, model bundle, data, source revision,
  execution policy, cost schedule, decision, research model, and evidence IDs;
- carries the simulated entry window, entry range, stop, target, quantity,
  planned loss, costs, reward/risk, expected R, probability status, thesis,
  bear case, cancellation rules, and rationale for a candidate;
- emits an explicit `NO_TRADE` notice when a complete run selects nothing;
- writes a create-once, content-addressed local notification with an exact
  message hash and idempotent retry semantics.

The local outbox is deliberately not a Telegram, email, WhatsApp, broker, or
webhook client. A later channel adapter must consume the immutable notification,
record its own idempotent delivery receipt, keep credentials outside the
artifact, and remain incapable of placing an order.

## Synthetic smoke test

From the repository root:

```powershell
$env:PYTHONPATH = "src"
python -m india_swing.demo `
  --output-dir var/audit `
  --shadow-outbox-dir var/shadow_outbox
```

The demo uses fictional symbols, synthetic evidence, provisional
probabilities, and `execution_eligible=false`. Its output must never be treated
as historical performance or a real recommendation.

The notification JSON is written below:

```text
var/shadow_outbox/notifications/<alert_id>.json
```

Publishing the exact same alert again returns the existing verified record.
Changed content receives a different alert ID. Tampered content, duplicate JSON
keys, unsafe paths, missing authority warnings, mutated nested decisions, and
conflicting same-ID content fail closed.
