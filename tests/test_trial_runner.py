from datetime import datetime, date, time, timedelta
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

from scripts.run_runner_trial import compute_future_slots


class TestTrialRunner(unittest.TestCase):
    def setUp(self):
        self.tz = ZoneInfo("Asia/Ho_Chi_Minh")

    def test_compute_future_slots_rounds_forward(self):
        """Slots must be spaced by exactly 5m and rounded forward, preserving safety delay."""
        # 14:02 + 12m delay = 14:14 -> rounded forward to 14:15
        now_vn = datetime(2026, 11, 18, 14, 2, 30, tzinfo=self.tz)
        target_date, slots_str, slots_dt = compute_future_slots(now_vn, delay_minutes=12)

        self.assertEqual(target_date, date(2026, 11, 18))
        self.assertEqual(slots_str, ["14:15", "14:20", "14:25"])
        self.assertTrue(slots_dt[0] < slots_dt[1] < slots_dt[2])

        # Verify delay is at least 12 minutes
        delay_actual = (slots_dt[0] - now_vn).total_seconds()
        self.assertGreaterEqual(delay_actual, 12 * 60)

    def test_midnight_crossing_shifts_to_tomorrow(self):
        """If slots would cross midnight, push to 00:15 tomorrow to keep all 3 in same date."""
        # 23:42 + 10m delay = 23:52 -> slot 3 would be 00:02 (crossing midnight)
        now_vn = datetime(2026, 11, 18, 23, 42, 0, tzinfo=self.tz)
        target_date, slots_str, slots_dt = compute_future_slots(now_vn, delay_minutes=10)

        # Shifted to tomorrow
        self.assertEqual(target_date, date(2026, 11, 19))
        self.assertEqual(slots_str, ["00:15", "00:20", "00:25"])
        self.assertEqual(slots_dt[0].date(), date(2026, 11, 19))
        self.assertEqual(slots_dt[1].date(), date(2026, 11, 19))
        self.assertEqual(slots_dt[2].date(), date(2026, 11, 19))

    def test_run_day_with_shared_ledger_and_new_trial_root_locking(self):
        """
        Regression test: run_day() must compute lock_path, create lock_path.parent
        and config.output('state') even when trial_root is brand-new without state/.
        Ensures anti-overlap lock is held on the shared state directory during execution.
        """
        import os
        import shutil
        import tempfile
        import unittest.mock
        import portalocker
        from src.config import load_config, ResolvedConfig
        from src.storage.state import State
        from scripts.manage_state import import_external_usage
        from src.workflow import run_day
        from src.preparation.plan import build_capture_plan
        from src.storage.files import read_json, write_json
        from src.domain.reference import RegistryPath
        from src.reference.graph import load_graph

        temp_dir = Path(tempfile.mkdtemp(prefix="test_trial_lock_"))
        try:
            # 1. Existing shared state ledger in separate location
            shared_state_file = temp_dir / "shared_pilot" / "state" / "capture.sqlite"
            shared_state = State(shared_state_file)
            import_external_usage(shared_state_file, "local_test_reconcile", date(2026, 11, 18), 33, 39, allow_init=True)
            self.assertEqual(shared_state.counts(), {"traffic": 33, "weather": 39})

            # 2. Brand-new trial root directory (NO state/ subdirectory exists!)
            trial_root = temp_dir / "runner_trial_3slots" / "trial_2026-11-18_1850_1900_test"
            self.assertFalse((trial_root / "state").exists())

            # 3. Build trial config
            base_cfg = load_config(Path("config/ci/study_13paths.yaml"))
            slots = ["18:50", "18:55", "19:00"]
            trial_plan_file = trial_root / "trial_plan.json"
            trial_study = base_cfg.study.model_copy(update={
                "study_id": "trial_lock_test",
                "slots": slots,
                "dates": [date(2026, 11, 18)],
                "pilot_run_root": trial_root,
                "capture_plan_file": trial_plan_file,
            })
            trial_cfg = ResolvedConfig(trial_study, base_cfg.policy, base_cfg.sources, base_cfg.budget)

            paths = [RegistryPath.model_validate(p) for p in read_json(trial_cfg.study.selected_paths_file)]
            graph = load_graph(trial_cfg.study.graph_cache)
            plan = build_capture_plan(paths, graph, trial_cfg)
            write_json(trial_plan_file, plan.model_dump(mode="json"), immutable=False)

            class DummyQueue:
                def __init__(self, *args, **kwargs):
                    self.errors = []
                def enqueue(self, record):
                    pass
                def finish(self):
                    pass

            def mock_run_schedule(day, plan, config, clock, state, budget, transport, *, on_record=None, progress=None):
                # Verify that the lock file is created and held on the SHARED state directory!
                expected_lock_file = shared_state_file.with_name("capture.lock")
                self.assertTrue(expected_lock_file.is_file())

                # Anti-overlap check: attempting to acquire lock concurrently on shared ledger MUST fail!
                with self.assertRaises((portalocker.AlreadyLocked, portalocker.LockException)):
                    with portalocker.Lock(expected_lock_file, timeout=0):
                        pass

                records = []
                for s in config.study.slots:
                    slot_tag = s.replace(":", "")
                    rec = {
                        "scheduled_at": f"{day.isoformat()}T{s}:00+07:00",
                        "status": "complete",
                        "snapshot_id": f"snap_{slot_tag}",
                    }
                    records.append(rec)
                    if on_record:
                        on_record(rec)
                return records

            os.environ["TOMTOM_API_KEY"] = "mock_key_for_test"
            with unittest.mock.patch("src.workflow.run_schedule", side_effect=mock_run_schedule):
                result = run_day(
                    trial_cfg,
                    date(2026, 11, 18),
                    allow_online=True,
                    state=shared_state,
                    queue_factory=DummyQueue,
                )

            # 4. Invariants check:
            # - run_day succeeded without FileNotFoundError
            self.assertEqual(result.get("status"), "complete")
            self.assertEqual(result.get("slots"), 3)

            # - trial_root/state directory was created cleanly
            self.assertTrue((trial_root / "state").is_dir())
            workflow_json = trial_root / f"state/workflow_2026-11-18_{plan.registry_version}.json"
            self.assertTrue(workflow_json.is_file())

            # - Shared ledger was preserved intact
            self.assertEqual(shared_state.counts(), {"traffic": 33, "weather": 39})

            # - Lock is released after run_day finishes
            expected_lock_file = shared_state_file.with_name("capture.lock")
            with portalocker.Lock(expected_lock_file, timeout=0):
                pass
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
