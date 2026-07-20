from __future__ import annotations

import ast
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.cloud_job import main

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DELEGATE_TARGET = "india_swing.cloud_job._daily_pipeline_main"


def _run_main(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        exit_code = main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


class ArgumentValidationTests(unittest.TestCase):
    def _assert_config_error(self, argv: list[str], secret: str | None = None) -> None:
        with patch(_DELEGATE_TARGET) as delegate:
            exit_code, stdout, stderr = _run_main(argv)
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(
            payload, {"status": "FAILED", "error_type": "CloudJobConfigurationError"}
        )
        delegate.assert_not_called()
        if secret is not None:
            self.assertNotIn(secret, stderr)

    def test_missing_spec_file(self) -> None:
        self._assert_config_error([])

    def test_unknown_flag(self) -> None:
        secret = "SECRET-UNKNOWN-FLAG-VALUE-DO-NOT-LEAK-1a2b"
        self._assert_config_error(["--unknown-flag", secret], secret=secret)

    def test_positional_argument(self) -> None:
        secret = "/secret/positional/spec-path-DO-NOT-LEAK.json"
        self._assert_config_error([secret], secret=secret)

    def test_duplicate_spec_file(self) -> None:
        secret = "/secret/second/spec-path-DO-NOT-LEAK.json"
        self._assert_config_error(
            ["--spec-file", "/first/spec.json", "--spec-file", secret], secret=secret
        )

    def test_spec_file_missing_value(self) -> None:
        self._assert_config_error(["--spec-file"])

    def test_empty_spec_file_value(self) -> None:
        self._assert_config_error(["--spec-file", ""])

    def test_extra_trailing_positional_after_valid_flag(self) -> None:
        secret = "/secret/trailing/positional-DO-NOT-LEAK.json"
        self._assert_config_error(["--spec-file", "/ok/spec.json", secret], secret=secret)


class DelegationTests(unittest.TestCase):
    def test_valid_spec_path_delegates_exactly_once_with_exact_argv(self) -> None:
        spec_path = "/tmp/operator-authored-spec.json"
        with patch(_DELEGATE_TARGET, return_value=0) as delegate:
            exit_code, stdout, stderr = _run_main(["--spec-file", spec_path])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        delegate.assert_called_once_with(["run-pinned-gcs", "--spec-file", spec_path])

    def test_nonzero_delegate_exit_code_passes_through_unchanged(self) -> None:
        with patch(_DELEGATE_TARGET, return_value=2):
            exit_code, stdout, stderr = _run_main(["--spec-file", "/tmp/spec.json"])
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_unexpected_secret_bearing_exception_from_delegate_is_sanitized(self) -> None:
        secret_path = "/secret/spec/path-DO-NOT-LEAK.json"
        secret_text = "SECRET-NESTED-EXCEPTION-TEXT-DO-NOT-LEAK-9f3c"

        def _raise(argv: list[str]) -> int:
            raise RuntimeError(f"failure reading {secret_path}: {secret_text}")

        with patch(_DELEGATE_TARGET, side_effect=_raise):
            exit_code, stdout, stderr = _run_main(["--spec-file", secret_path])
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(payload, {"status": "FAILED", "error_type": "CloudJobRuntimeError"})
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertNotIn(secret_path, stderr)
        self.assertNotIn(secret_text, stderr)


class CapabilityLockTests(unittest.TestCase):
    def _module_ast(self) -> ast.Module:
        source = (_REPO_ROOT / "src" / "india_swing" / "cloud_job.py").read_text(
            encoding="utf-8"
        )
        return ast.parse(source)

    def test_imports_no_demo_forecasting_research_signals_market_data_or_broker_order(
        self,
    ) -> None:
        forbidden_segments = {"demo", "forecasting", "research", "signals", "market_data"}
        forbidden_substrings = ("broker", "order")
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                lowered = name.lower()
                segments = set(lowered.split("."))
                self.assertTrue(segments.isdisjoint(forbidden_segments), name)
                for token in forbidden_substrings:
                    self.assertNotIn(token, lowered, name)


class DockerfileTests(unittest.TestCase):
    def test_effective_final_cmd_is_cloud_job_and_never_invokes_demo(self) -> None:
        text = (_REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        runtime_command_lines = [
            line
            for line in text.splitlines()
            if line.strip().startswith("CMD") or line.strip().startswith("ENTRYPOINT")
        ]
        self.assertTrue(runtime_command_lines, "Dockerfile has no CMD/ENTRYPOINT instruction")
        for line in runtime_command_lines:
            self.assertNotIn("india_swing.demo", line)
        final_line = runtime_command_lines[-1]
        self.assertIn("india_swing.cloud_job", final_line)


_UNUSED_LEGACY_SECRETS = (
    "KITE_API_KEY",
    "KITE_API_SECRET",
    "LLM_API_KEY",
    "NOTIFICATION_TOKEN",
)


class DeployScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (_REPO_ROOT / "deploy.sh").read_text(encoding="utf-8")

    def _job_blocks(self) -> tuple[str, str]:
        update_start = self.text.index('gcloud run jobs update "${JOB_NAME}"')
        create_start = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        self.assertLess(update_start, create_start)
        update_block = self.text[update_start:create_start]
        # The create block runs from its own start to the closing `fi` of
        # the surrounding if/else that selects update vs. create.
        fi_after_create = self.text.index("\nfi", create_start)
        create_block = self.text[create_start:fi_after_create]
        return update_block, create_block

    def test_scheduler_flag_defaults_false(self) -> None:
        self.assertIn('ENABLE_EOD_SCHEDULER="${ENABLE_EOD_SCHEDULER:-false}"', self.text)

    def test_enable_eod_scheduler_true_fails_closed_before_any_gcloud_mutation(self) -> None:
        reject_marker = 'if [ "${ENABLE_EOD_SCHEDULER}" = "true" ]'
        reject_index = self.text.index(reject_marker)
        reject_fi = self.text.index("\nfi", reject_index)
        reject_block = self.text[reject_index:reject_fi]
        self.assertIn("exit 1", reject_block)
        self.assertNotIn("gcloud", reject_block)

        first_services_enable = self.text.index("gcloud services enable")
        first_job_create = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        self.assertLess(reject_index, first_services_enable)
        self.assertLess(reject_index, first_job_create)

    def test_no_reachable_eod_scheduler_create_update_resume_command(self) -> None:
        for needle in (
            "gcloud scheduler jobs create http eod-swing-schedule",
            "gcloud scheduler jobs update http eod-swing-schedule",
            "gcloud scheduler jobs resume",
        ):
            self.assertNotIn(needle, self.text)

    def test_eod_scheduler_section_pauses_when_present_and_reports_disabled_otherwise(
        self,
    ) -> None:
        section_marker = "# B. EOD Swing Job Schedule"
        section_index = self.text.index(section_marker)
        pause_if_index = self.text.index(
            'if gcloud scheduler jobs describe "eod-swing-schedule"', section_index
        )
        else_index = self.text.index("\nelse", pause_if_index)
        fi_index = self.text.index("\nfi", else_index)
        true_branch = self.text[pause_if_index:else_index]
        false_branch = self.text[else_index:fi_index]

        self.assertIn("gcloud scheduler jobs pause", true_branch)
        self.assertIn('EOD_SCHEDULER_STATE="paused"', true_branch)
        self.assertNotIn("gcloud scheduler jobs pause", false_branch)
        self.assertIn('EOD_SCHEDULER_STATE="disabled"', false_branch)

    def test_pinned_run_spec_secret_version_validated_before_build_or_job_mutation(
        self,
    ) -> None:
        version_var = "PINNED_GCS_RUN_SPEC_SECRET_VERSION"
        self.assertIn(f'{version_var}="${{{version_var}:-}}"', self.text)
        regex_check_index = self.text.index("^[1-9][0-9]*$")
        regex_line_start = self.text.rindex("\n", 0, regex_check_index)
        regex_block_end = self.text.index("\nfi", regex_check_index)
        regex_block = self.text[regex_line_start:regex_block_end]
        self.assertIn("exit 1", regex_block)

        docker_build_index = self.text.index("docker build")
        cloud_build_index = self.text.index("gcloud builds submit")
        job_create_index = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        job_update_index = self.text.index('gcloud run jobs update "${JOB_NAME}"')
        self.assertLess(regex_check_index, docker_build_index)
        self.assertLess(regex_check_index, cloud_build_index)
        self.assertLess(regex_check_index, job_create_index)
        self.assertLess(regex_check_index, job_update_index)

    def test_pinned_run_spec_secret_never_created_seeded_or_hashed(self) -> None:
        secret_name = "PINNED_GCS_RUN_SPEC"
        self.assertIn(f'PINNED_RUN_SPEC_SECRET_NAME="{secret_name}"', self.text)
        self.assertNotIn(f'gcloud secrets create "{secret_name}"', self.text)
        self.assertNotIn(f'gcloud secrets create "${{PINNED_RUN_SPEC_SECRET_NAME}}"', self.text)
        # The existing placeholder-seeding loop only ever iterates the four
        # unused legacy secrets, never the pinned run-spec secret name.
        seed_loop_start = self.text.index('for secret in "${SECRETS[@]}"; do')
        seed_loop_end = self.text.index("\ndone", seed_loop_start)
        seed_loop = self.text[seed_loop_start:seed_loop_end]
        self.assertNotIn("PINNED_RUN_SPEC_SECRET_NAME", seed_loop)
        self.assertNotIn(secret_name, seed_loop)
        self.assertNotIn("sha256", self.text.lower())
        self.assertIn("gcloud secrets describe \"${PINNED_RUN_SPEC_SECRET_NAME}\"", self.text)
        self.assertIn("gcloud secrets versions describe", self.text)

        iam_grant_index = self.text.index(
            'gcloud secrets add-iam-policy-binding "${PINNED_RUN_SPEC_SECRET_NAME}"'
        )
        iam_grant_block_end = self.text.index("\n\n", iam_grant_index)
        iam_grant_block = self.text[iam_grant_index:iam_grant_block_end]
        self.assertIn("roles/secretmanager.secretAccessor", iam_grant_block)
        for legacy_secret in _UNUSED_LEGACY_SECRETS:
            self.assertNotIn(
                f'gcloud secrets add-iam-policy-binding "{legacy_secret}"', self.text
            )

    def test_legacy_secret_accessor_bindings_are_revoked_before_job_mutation(
        self,
    ) -> None:
        revoke_loop_start = self.text.index('for secret in "${SECRETS[@]}"; do', self.text.index("# Revoke bindings"))
        revoke_loop_end = self.text.index("\ndone", revoke_loop_start)
        revoke_loop = self.text[revoke_loop_start:revoke_loop_end]
        self.assertIn('gcloud secrets remove-iam-policy-binding "${secret}"', revoke_loop)
        self.assertIn(
            '--member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com"',
            revoke_loop,
        )
        self.assertIn('--role="roles/secretmanager.secretAccessor"', revoke_loop)
        self.assertNotIn("|| true", revoke_loop)
        self.assertLess(
            revoke_loop_start,
            self.text.index('gcloud run jobs update "${JOB_NAME}"'),
        )
        self.assertLess(
            revoke_loop_start,
            self.text.index('gcloud run jobs create "${JOB_NAME}"'),
        )

    def test_job_blocks_use_pinned_spec_file_argument_and_fixed_mount_path(self) -> None:
        self.assertIn(
            'PINNED_RUN_SPEC_MOUNT_PATH="/var/run/india-swing-control/pinned-run-spec.json"',
            self.text,
        )
        for block in self._job_blocks():
            self.assertIn(
                '--args=-m,india_swing.cloud_job,--spec-file,"${PINNED_RUN_SPEC_MOUNT_PATH}"',
                block,
            )

    def test_job_blocks_set_secrets_to_numerically_pinned_mapping_with_no_latest(
        self,
    ) -> None:
        for block in self._job_blocks():
            self.assertIn(
                '--set-secrets="${PINNED_RUN_SPEC_MOUNT_PATH}='
                '${PINNED_RUN_SPEC_SECRET_NAME}:${PINNED_GCS_RUN_SPEC_SECRET_VERSION}"',
                block,
            )
            self.assertNotIn("latest", block)
            for legacy_secret in _UNUSED_LEGACY_SECRETS:
                self.assertNotIn(legacy_secret, block)

    def test_job_blocks_set_distinct_ephemeral_tmp_artifact_roots(self) -> None:
        expected_roots = {
            "INDIA_SWING_CALENDAR_DATA_ROOT": "/tmp/india-swing/calendar_data",
            "INDIA_SWING_IDENTITY_REGISTRY_ROOT": "/tmp/india-swing/identity_registry",
            "INDIA_SWING_HISTORICAL_PRICES_ROOT": "/tmp/india-swing/historical_prices",
            "INDIA_SWING_DAILY_REPORTS_ROOT": "/tmp/india-swing/daily_reports",
            "INDIA_SWING_REFERENCE_DATA_ROOT": "/tmp/india-swing/reference_data",
            "INDIA_SWING_DAILY_PIPELINE_ROOT": "/tmp/india-swing/daily_pipeline",
        }
        self.assertEqual(len(set(expected_roots.values())), len(expected_roots))
        artifact_roots_index = self.text.index("PINNED_JOB_ARTIFACT_ROOTS=")
        artifact_roots_line_end = self.text.index("\n", artifact_roots_index)
        artifact_roots_line = self.text[artifact_roots_index:artifact_roots_line_end]
        for env_name, path in expected_roots.items():
            self.assertIn(f"{env_name}={path}", artifact_roots_line)
            self.assertTrue(path.startswith("/tmp/india-swing/"))
        for block in self._job_blocks():
            self.assertIn("${PINNED_JOB_ARTIFACT_ROOTS}", block)
        ephemeral_comment_index = self.text.index("ephemeral")
        self.assertLess(ephemeral_comment_index, artifact_roots_index)

    def test_state_publication_bucket_env_mapped_from_exact_bucket_name_before_job_blocks(
        self,
    ) -> None:
        self.assertIn(
            'PINNED_JOB_STATE_PUBLICATION_BUCKET_ENV='
            '"INDIA_SWING_STATE_PUBLICATION_BUCKET=${BUCKET_NAME}"',
            self.text,
        )
        self.assertEqual(self.text.count("INDIA_SWING_STATE_PUBLICATION_BUCKET"), 1)

        mapping_index = self.text.index("PINNED_JOB_STATE_PUBLICATION_BUCKET_ENV=")
        job_update_index = self.text.index('gcloud run jobs update "${JOB_NAME}"')
        job_create_index = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        self.assertLess(mapping_index, job_update_index)
        self.assertLess(mapping_index, job_create_index)

    def test_job_blocks_include_state_publication_bucket_in_set_env_vars(self) -> None:
        for block in self._job_blocks():
            self.assertIn(
                'BUCKET_NAME=${BUCKET_NAME},FIRESTORE_DATABASE=${FIRESTORE_DATABASE},'
                '${PINNED_JOB_ARTIFACT_ROOTS},${PINNED_JOB_STATE_PUBLICATION_BUCKET_ENV}',
                block,
            )

    def test_state_publication_bucket_never_in_set_secrets_or_args(self) -> None:
        for block in self._job_blocks():
            args_index = block.index("--args=")
            args_line_end = block.index("\n", args_index)
            args_line = block[args_index:args_line_end]
            self.assertNotIn("STATE_PUBLICATION_BUCKET", args_line)
            self.assertNotIn("PINNED_JOB_STATE_PUBLICATION_BUCKET_ENV", args_line)

            secrets_index = block.index("--set-secrets=")
            secrets_line_end = block.find("\n", secrets_index)
            if secrets_line_end == -1:
                secrets_line_end = len(block)
            secrets_line = block[secrets_index:secrets_line_end]
            self.assertNotIn("STATE_PUBLICATION_BUCKET", secrets_line)
            self.assertNotIn("PINNED_JOB_STATE_PUBLICATION_BUCKET_ENV", secrets_line)
            self.assertNotIn("latest", secrets_line)

    def test_eod_swing_runtime_objectuser_grant_is_bucket_scoped_before_job_blocks(
        self,
    ) -> None:
        grant_marker = "# B. EOD Swing Runtime Permissions"
        grant_index = self.text.index(grant_marker)
        grant_block_end = self.text.index("\n\n", grant_index)
        grant_block = self.text[grant_index:grant_block_end]
        self.assertIn(
            'gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}"',
            grant_block,
        )
        self.assertIn(
            '--member="serviceAccount:${JOB_RUNTIME_SERVICE_ACCOUNT}'
            '@${PROJECT_ID}.iam.gserviceaccount.com"',
            grant_block,
        )
        self.assertIn('--role="roles/storage.objectUser"', grant_block)
        self.assertNotIn("roles/storage.admin", grant_block)

        job_update_index = self.text.index('gcloud run jobs update "${JOB_NAME}"')
        job_create_index = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        self.assertLess(grant_index, job_update_index)
        self.assertLess(grant_index, job_create_index)

    def test_final_messages_do_not_unconditionally_claim_schedulers_active(self) -> None:
        success_index = self.text.rindex("SUCCESS:")
        tail = self.text[success_index:]
        self.assertNotIn("Your Scheduler jobs are now active", tail)
        self.assertIn("EOD_SCHEDULER_STATE", tail)

    def test_no_test_or_implementation_execution_of_deploy_script(self) -> None:
        # This test class only ever reads deploy.sh as text; it never
        # imports subprocess, never shells out, and never invokes bash,
        # docker, or gcloud. Asserting the text was read successfully (and
        # is non-empty) is the whole extent of what this test exercises.
        self.assertGreater(len(self.text), 0)


if __name__ == "__main__":
    unittest.main()
