# HYP-002 quality pilot: operator checklist

Status: this document describes the **local control-plane layer** built by
`src/india_swing/quality_pilot/arming.py`, `deployment_plan.py`,
`quality_pilot_arming_cli.py`, and the `run-due-window` command added to
`quality_pilot_job.py`. **No deployment exists yet.** Nothing in this
increment calls `gcloud`, creates a Cloud Run Job, creates a Cloud
Scheduler job, or grants any deployment authority — `render_quality_pilot_deployment_plan`
renders canonical JSON *data*, not an executable script, and every arming
manifest exposes a fixed `armed=False` field that this module can never set
to `True`. Applying the rendered plan to real GCP infrastructure is a
separate, later, human-authorized increment that Codex reviews independently.

The pilot is quality-only and permanently excluded from O0 and every
research/training/feature/label/signal/paper/notification/execution/capital
path. It stops after exactly 20 confirmed NSE sessions.

## Founder daily checklist

Complete these steps in order, once per trading day, before the scheduled
lanes fire:

1. **Complete the Kite daily login manually.** This system has no
   interactive/browser login flow and no token-refresh capability — the
   access token must come from the founder's own manual Kite Connect login
   each trading day.
2. **Add or update the exact access-token Secret Manager version.** Create a
   new secret version holding the fresh daily token. **Never paste the raw
   token into chat, a commit, a log line, or any file this repository
   tracks.** The arming manifest only ever carries a secret *reference*
   (secret id + numeric version) — never a value.
3. **Update the arming manifest and its `environment_sha256`/image digest
   through the separately reviewed deployment controls** (not by hand-
   editing a mounted file) whenever the token's secret version, the
   container image digest, or the code/environment identity changes. A
   stale manifest whose digests disagree with the running container's own
   identity is rejected by `run-due-window` before any window is ever
   delegated.
4. **Confirm calendar/event admission for today's session.** The runbook's
   20 confirmed sessions and their calendar decision ids are fixed at
   arming time; today's session must already be one of those 20 dates, and
   its calendar decision must already be separately admitted. This system
   never infers, backfills, or invents a session or calendar decision.
5. **Confirm the kill switch.** `INDIA_SWING_QUALITY_PILOT_ARMED` must
   equal the exact lowercase literal `true` in the running container's
   environment. Anything else — missing, `False`, `1`, `TRUE`, or any
   other value — disarms every lane with zero credential, GCS, Kite,
   claim, collector, or writer capability. Flip this only when the founder
   has actually completed steps 1-4 for today.
6. **Inspect the previous window's completion** before trusting today's
   run. `run-due-window` reports one of five sanitized postures on stdout:
   `QUALITY_PILOT_DISARMED`, `QUALITY_PILOT_NOT_SCHEDULED`,
   `QUALITY_PILOT_ALREADY_COMPLETE`, a genuine `QUALITY_PILOT_WINDOW_*`
   result (delegated to the accepted window service), or a failure written
   to stderr. Confirm the last several scheduled firings each ended in one
   of the expected postures for that day's stage of the pilot.
7. **Stop on any missed or indeterminate window.** A window whose
   `closes_at` has passed without an independently verified terminal
   completion never silently advances to a later window or session, and a
   crashed claim never triggers an automatic recollection. If a firing
   fails or reports something other than the expected posture, stop and
   investigate by hand before the next scheduled firing — do not restart,
   re-arm, or edit the mounted runbook/manifest to work around it.

## What this increment does not do

- It does not deploy, create, or modify a Cloud Run Job, Cloud Scheduler
  job, IAM binding, or Secret Manager secret.
- It does not call `gcloud`, `docker`, Terraform, or any subprocess.
- It does not read `.env`, a secret value, a credential, or the network.
- It does not perform interactive/browser Kite login or token refresh.
- It does not retry a claimed action, widen a byte/row/timing ceiling, or
  treat a missed window as a provider gap.
- It does not generate a signal, label, paper trade, notification, order,
  or grant any capital authority. The pilot's own captured observations
  remain permanently excluded from O0 and every research/training/
  feature/label/alert/execution path.
