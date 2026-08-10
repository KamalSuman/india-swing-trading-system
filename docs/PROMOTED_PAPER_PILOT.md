# Promoted paper pilot

`india-swing-promoted-paper-pilot-job` is the manually triggered production
boundary for the promoted engine. It composes the accepted hydrated Cloud Run
job, verifies its terminal state was durably published, loads the matching
sealed advisory and terminal from the hydrated runtime, and only then delivers
the advisory to the configured private Telegram chat.

It remains paper-only. It cannot place, modify, or cancel a broker order. The
engine's records remain `notification_eligible=false` and
`execution_eligible=false`; the outer operator-configured pilot is the separate
authority to deliver an already-sealed research advisory.

## Safety and restart behavior

Before calling Telegram, the job creates one exact GCS claim at:

```text
promoted-paper-pilot-notifications/v1/<session>/<terminal-id>/<chat-binding-id>/claim.json
```

After Telegram confirms the message, the exact receipt is stored beside it as
`receipt.json`. A later invocation with the same terminal and chat replays the
durable receipt without contacting Telegram. If a process stops after the claim
but before the receipt, delivery is marked uncertain and is never retried
automatically. This deliberately prefers a potentially missed alert over a
duplicate alert after an ambiguous network outcome.

The final success envelope contains the hydrated job's exact state-manifest
coordinates plus `notification_claim_id`, `notification_receipt_id`,
`telegram_receipt_id`, and `notification_replayed`. It never emits the bot
token, chat ID, local paths, raw advisory bytes, or nested exception text.
Its exact success status is `PROMOTED_PAPER_PILOT_JOB_COMPLETE`.

## Container/manual invocation

First prepare the source control and publish the immutable input snapshot as
described in `docs/PROMOTED_OPERATIONAL_CLOUD_CONTROL_PREPARE.md` and
`docs/PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB.md`. Then set the four runtime
values without printing them:

```bash
export INDIA_SWING_KITE_API_KEY="<kite-api-key>"
export INDIA_SWING_KITE_ACCESS_TOKEN="<today-access-token>"
export INDIA_SWING_TELEGRAM_BOT_TOKEN="<telegram-bot-token>"
export INDIA_SWING_TELEGRAM_CHAT_ID="<private-chat-id>"
export INDIA_SWING_PAPER_PILOT_STATE_BUCKET="<exact-launch-state-bucket>"
python -m india_swing.promoted_paper_pilot_job \
  --launch-file /var/run/india-swing/promoted-launch.json
```

The production runtime layout is intentionally fixed beneath
`/tmp/india-swing`, so this final command is for the Linux container. Use the
offline Windows commands in the control-preparation and input-publication docs
to create `hydrated-launch.json`; do not run this final process directly in a
Windows host filesystem.

The Kite access token is still a daily operational input. This job does not
perform browser login, TOTP automation, or token refresh.

## GCP deployment

Build and push the repository image, resolve its immutable digest URI, create
five Secret Manager values (launch file, Kite API key, daily Kite access token,
Telegram bot token, Telegram chat ID), and set the exact version variables
listed by `infra/deploy_promoted_paper_pilot.ps1`. Then run that script.

Create the launch secret once and add each new session as an explicit version:

```powershell
gcloud secrets create india-swing-promoted-launch --replication-policy=automatic
gcloud secrets versions add india-swing-promoted-launch `
  --data-file=C:\absolute\run\hydrated-launch.json
```

Record the returned numeric version and set
`INDIA_SWING_PAPER_PILOT_LAUNCH_SECRET_VERSION` to that exact number. Never use
`latest` for the launch, Kite token, or Telegram configuration.

The deployment uses one runtime service account, digest-pins the image,
version-pins every secret, grants only GCS object access plus access to those
five secrets, independently binds the expected state bucket as a non-secret
environment value, configures zero Cloud Run retries, and leaves scheduling disabled.
Execute the job manually with:

```powershell
gcloud run jobs execute india-swing-promoted-paper-pilot `
  --region asia-south1 `
  --project $env:GCP_PROJECT_ID `
  --wait
```

Do not enable a recurring scheduler for a static launch file. The launch binds
one exact session; automatic daily rollover requires a separately reviewed
per-session controller.
