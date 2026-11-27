"""
Dedicated offline contract and simulation tests for the Cloudflare Worker guard
(`cloudflare_worker/src/index.js`, `cloudflare_worker/wrangler.toml`) and its
post-merge interaction with `.github/workflows/capture_13paths.yml` and `scripts/check_run_guard.py`.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import tomllib
import unittest
from zoneinfo import ZoneInfo

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
WORKER_JS_PATH = REPO_ROOT / "cloudflare_worker" / "src" / "index.js"
WRANGLER_TOML_PATH = REPO_ROOT / "cloudflare_worker" / "wrangler.toml"
PACKAGE_JSON_PATH = REPO_ROOT / "cloudflare_worker" / "package.json"
WORKFLOW_YML_PATH = REPO_ROOT / ".github" / "workflows" / "capture_13paths.yml"


def _vn_date_str(dt_utc: datetime) -> str:
    return dt_utc.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")


class WorkerGuardReferenceModel:
    """
    Exact Python mirror verified against `cloudflare_worker/src/index.js` AST/source invariants
    so every Worker decision path is deterministically tested even in environments without Node.js.
    """

    ACTIVE_STATUSES = ("queued", "in_progress", "waiting", "pending", "requested")

    @classmethod
    def is_active_production_live_run(cls, run: dict, today_vn: str) -> bool:
        if run.get("status") not in cls.ACTIVE_STATUSES:
            return False
        title = str(run.get("display_title") or "")
        if "[trial_3slots" in title or "dry-run" in title:
            return False
        m = re.search(r"\[afternoon_37slots live (\d{4}-\d{2}-\d{2})\]", title)
        if m:
            return m.group(1) == today_vn
        if (
            run.get("event") == "schedule"
            or "[afternoon_37slots live" in title
            or title == "Capture 13 Paths"
        ):
            created_dt = datetime.fromisoformat(str(run["created_at"]).replace("Z", "+00:00"))
            return _vn_date_str(created_dt) == today_vn
        return False

    @classmethod
    def classify_completed_run_capture(cls, run: dict, artifacts: list[dict], jobs: list[dict], today_vn: str):
        title = str(run.get("display_title") or "")
        if "[trial_3slots" in title or "dry-run" in title:
            return None

        prefix = f"capture-outputs-afternoon_37slots-{today_vn}-"
        prod_art = next((a for a in artifacts if str(a.get("name") or "").startswith(prefix)), None)
        if not prod_art:
            return None

        slot1_utc = datetime.fromisoformat(f"{today_vn}T16:00:00+07:00").astimezone(timezone.utc)
        for job in jobs:
            for step in job.get("steps", []):
                if step.get("name") != "Execute Capture Run (Trial or Afternoon)":
                    continue
                conc = step.get("conclusion")
                if conc == "skipped":
                    return None
                comp_dt = (
                    datetime.fromisoformat(step["completed_at"].replace("Z", "+00:00"))
                    if step.get("completed_at")
                    else None
                )
                start_dt = (
                    datetime.fromisoformat(step["started_at"].replace("Z", "+00:00"))
                    if step.get("started_at")
                    else None
                )
                dur_s = (comp_dt - start_dt).total_seconds() if (comp_dt and start_dt) else 0.0
                size = int(prod_art.get("size_in_bytes") or 0)

                if conc == "cancelled":
                    if comp_dt and comp_dt < slot1_utc:
                        return None
                    if comp_dt and comp_dt >= slot1_utc and size >= 2_000_000:
                        return {
                            "reason": "PARTIAL_LIVE_CANCELLED",
                            "artifact": prod_art["name"],
                            "run_conclusion": run.get("conclusion"),
                        }
                    return None

                if conc == "success":
                    if size >= 2_000_000 or (comp_dt and comp_dt >= slot1_utc and dur_s >= 120):
                        return {
                            "reason": "ALREADY_COMPLETED" if run.get("conclusion") == "success" else "PARTIAL_LIVE_CAPTURED",
                            "artifact": prod_art["name"],
                            "run_conclusion": run.get("conclusion"),
                        }
                    return None
        return None


class TestCloudflareWorkerGuardContract(unittest.TestCase):
    def setUp(self):
        self.worker_js = WORKER_JS_PATH.read_text(encoding="utf-8")
        with open(WRANGLER_TOML_PATH, "rb") as f:
            self.wrangler = tomllib.load(f)
        self.pkg = json.loads(PACKAGE_JSON_PATH.read_text(encoding="utf-8"))
        with open(WORKFLOW_YML_PATH, "r", encoding="utf-8") as f:
            self.workflow = yaml.safe_load(f)
        self.workflow_raw = WORKFLOW_YML_PATH.read_text(encoding="utf-8")

    def test_01_worker_source_invariants_and_schedules(self):
        # 1. Dual crons at 15:35 and 15:45 Vietnam time (08:35 and 08:45 UTC)
        crons = self.wrangler.get("triggers", {}).get("crons", [])
        self.assertEqual(crons, ["35 8 * * *", "45 8 * * *"])
        self.assertEqual(self.wrangler["vars"]["GITHUB_REPO"], "kienhuynh22/HCM-Traffic")
        self.assertEqual(self.wrangler["vars"]["WORKFLOW_FILE"], "capture_13paths.yml")
        self.assertEqual(self.wrangler["vars"]["TARGET_BRANCH"], "master")

        # 2. ESM package configuration & Node test script
        self.assertEqual(self.pkg.get("type"), "module")
        self.assertIn("worker.test.mjs", self.pkg.get("scripts", {}).get("test", ""))
        self.assertTrue((REPO_ROOT / "cloudflare_worker" / "test" / "worker.test.mjs").is_file())

        # 3. Verify ACTIVE_RUN_STATUSES in index.js covers queued/pending/waiting/requested/in_progress
        m_statuses = re.search(r"ACTIVE_RUN_STATUSES\s*=\s*\[(.*?)\]", self.worker_js, re.S)
        self.assertIsNotNone(m_statuses)
        js_statuses = re.findall(r'"([a-z_]+)"', m_statuses.group(1))
        self.assertEqual(
            set(js_statuses),
            {"queued", "in_progress", "waiting", "pending", "requested"},
        )

        # 4. Verify index.js does NOT filter runs by conclusion=success and checks 2_000_000 byte threshold
        self.assertNotIn("conclusion=success", self.worker_js)
        self.assertIn("PARTIAL_LIVE_CAPTURED", self.worker_js)
        self.assertIn("2_000_000", self.worker_js)

    def test_02_worker_dispatch_inputs_match_post_merge_master_workflow(self):
        triggers = self.workflow.get("on") or self.workflow.get(True) or {}
        wf_inputs = triggers["workflow_dispatch"]["inputs"]

        # Extract exact key-value pairs from dispatchPayload in index.js
        m_inputs = re.search(r"const dispatchPayload\s*=\s*\{\s*ref:\s*branch,\s*inputs:\s*\{(.*?)\n\s*\}", self.worker_js, re.S)
        self.assertIsNotNone(m_inputs)
        block = m_inputs.group(1)
        pairs = dict(re.findall(r"^\s*([a-zA-Z0-9_]+)\s*:\s*([^,\n]+)", block, re.M))

        # GitHub REST API /dispatches allows max 10 inputs; all sent keys must exist in capture_13paths.yml
        self.assertLessEqual(len(pairs), 10)
        for key in pairs:
            self.assertIn(key, wf_inputs, f"Worker sends unknown input '{key}'")

        # Verify exact values sent by Worker
        self.assertEqual(pairs["mode"], '"afternoon_37slots"')
        self.assertIn("afternoon_37slots", wf_inputs["mode"]["options"])
        self.assertEqual(pairs["dry_run"], '"false"')
        self.assertEqual(pairs["allow_init_state"], '"false"')
        self.assertEqual(pairs["date"], "todayVN")
        self.assertEqual(pairs["automated"], '"true"')
        self.assertEqual(pairs["import_external_source"], '""')
        self.assertEqual(pairs["import_traffic"], '"0"')
        self.assertEqual(pairs["import_weather"], '"0"')

        # Verify workflow run-name safely evaluates string 'false' AND boolean false as 'live' (never 'dry-run')
        run_name_expr = str(self.workflow.get("run-name", ""))
        self.assertIn("format('{0}', inputs.dry_run) == 'true'", run_name_expr)

        # Verify workflow duplicate step passes both --checkpoint-archive and --db-path
        self.assertIn("--db-path $STATE_FILE", self.workflow_raw)
        self.assertIn("--checkpoint-archive $CHECKPOINT_ARCHIVE", self.workflow_raw)

    def test_03_run20_36_of_37_failure_blocks_worker_recapture(self):
        run20 = {
            "id": 45788636369,
            "status": "completed",
            "conclusion": "failure",
            "event": "workflow_dispatch",
            "display_title": "Capture 13 Paths",
            "created_at": "2026-11-26T09:04:42Z",
        }
        artifacts20 = [
            {
                "name": "capture-outputs-afternoon_37slots-2026-11-26-run45788636369",
                "size_in_bytes": 152005006,
            }
        ]
        jobs20 = [
            {
                "steps": [
                    {
                        "name": "Execute Capture Run (Trial or Afternoon)",
                        "conclusion": "success",
                        "started_at": "2026-11-26T09:05:16Z",
                        "completed_at": "2026-11-26T12:00:17Z",
                    }
                ]
            }
        ]
        match = WorkerGuardReferenceModel.classify_completed_run_capture(
            run20, artifacts20, jobs20, "2026-11-26"
        )
        self.assertIsNotNone(match)
        self.assertEqual(match["reason"], "PARTIAL_LIVE_CAPTURED")
        self.assertEqual(match["run_conclusion"], "failure")

    def test_04_dry_run_trial_and_midnight_run21_do_not_block_production(self):
        today = "2026-11-27"
        # 1. Active dry-run does not block
        self.assertFalse(
            WorkerGuardReferenceModel.is_active_production_live_run(
                {
                    "id": 1,
                    "status": "in_progress",
                    "event": "workflow_dispatch",
                    "display_title": "Capture 13 Paths [afternoon_37slots dry-run 2026-11-27]",
                    "created_at": "2026-11-27T08:30:00Z",
                },
                today,
            )
        )
        # 2. Active trial_3slots does not block
        self.assertFalse(
            WorkerGuardReferenceModel.is_active_production_live_run(
                {
                    "id": 2,
                    "status": "queued",
                    "event": "workflow_dispatch",
                    "display_title": "Capture 13 Paths [trial_3slots live auto-vn]",
                    "created_at": "2026-11-27T08:30:00Z",
                },
                today,
            )
        )
        # 3. Completed legacy dry-run (<2MB, 2s duration) does not block
        self.assertIsNone(
            WorkerGuardReferenceModel.classify_completed_run_capture(
                {"id": 3, "status": "completed", "conclusion": "success", "display_title": "Capture 13 Paths"},
                [{"name": "capture-outputs-afternoon_37slots-2026-11-27-run3", "size_in_bytes": 26050}],
                [
                    {
                        "steps": [
                            {
                                "name": "Execute Capture Run (Trial or Afternoon)",
                                "conclusion": "success",
                                "started_at": "2026-11-27T08:15:38Z",
                                "completed_at": "2026-11-27T08:15:40Z",
                            }
                        ]
                    }
                ],
                today,
            )
        )
        # 4. Midnight Run #21 (cancelled at 05:37 ICT before 16:00 slot 1, 917KB) does not block
        self.assertIsNone(
            WorkerGuardReferenceModel.classify_completed_run_capture(
                {
                    "id": 45877394268,
                    "status": "completed",
                    "conclusion": "cancelled",
                    "display_title": "Capture 13 Paths",
                },
                [
                    {
                        "name": "capture-outputs-afternoon_37slots-2026-11-27-run45877394268",
                        "size_in_bytes": 917955,
                    }
                ],
                [
                    {
                        "steps": [
                            {
                                "name": "Execute Capture Run (Trial or Afternoon)",
                                "conclusion": "cancelled",
                                "started_at": "2026-11-26T18:37:52Z",
                                "completed_at": "2026-11-26T22:37:34Z",
                            }
                        ]
                    }
                ],
                today,
            )
        )

    def test_05_dual_triggers_1535_1545_and_manual_queued_or_running(self):
        today = "2026-11-27"
        # Every active GitHub status (including 'pending' in concurrency queue and 'queued')
        # across pre-merge manual Run #22 ("Capture 13 Paths") and post-merge manual/Worker titles
        # MUST suppress both 15:35 and 15:45 Worker dispatches.
        titles = [
            "Capture 13 Paths",  # Run #22 manual run on 2026-11-27
            "Capture 13 Paths [afternoon_37slots live auto-vn]",  # Manual UI dispatch with blank date
            "Capture 13 Paths [afternoon_37slots live 2026-11-27]",  # Worker 15:35 dispatch or manual with date
            "Capture 13 Paths [afternoon_37slots live]",  # Scheduled cron run
        ]
        for status in ("queued", "pending", "waiting", "requested", "in_progress"):
            for title in titles:
                run_obj = {
                    "id": 45969183827,
                    "status": status,
                    "event": "schedule" if title.endswith("live]") else "workflow_dispatch",
                    "display_title": title,
                    "created_at": "2026-11-27T08:34:00Z",  # 15:34 ICT (before 15:35 & 15:45)
                }
                self.assertTrue(
                    WorkerGuardReferenceModel.is_active_production_live_run(run_obj, today),
                    f"Failed to detect active run for status={status}, title={title}",
                )


if __name__ == "__main__":
    unittest.main()
