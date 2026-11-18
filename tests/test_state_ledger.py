import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path
import shutil
import sqlite3
import tempfile
from zoneinfo import ZoneInfo

from src.storage.state import State
from scripts.manage_state import (
    import_external_usage,
    create_checkpoint,
    restore_checkpoint,
    flush_and_verify_sqlite,
)


class TestStateLedger(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_ledger_"))
        self.state_file = self.test_dir / "state" / "capture.sqlite"
        self.checkpoint_dir = self.test_dir / "checkpoints"
        self.tz = "Asia/Ho_Chi_Minh"
        self.target_date = date(2026, 11, 18)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_a_new_ledger_plus_local_source(self):
        """a. New ledger + local source 33/39 -> total 33/39."""
        res = import_external_usage(
            self.state_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=True,
        )
        self.assertEqual(res["traffic"]["status"], "imported")
        self.assertEqual(res["traffic"]["count"], 33)
        self.assertEqual(res["weather"]["status"], "imported")
        self.assertEqual(res["weather"]["count"], 39)

        state = State(self.state_file)
        dt_target = datetime.combine(self.target_date, time(12, 0), ZoneInfo(self.tz)).astimezone(timezone.utc)
        daily = state.daily_counts(dt_target, self.tz)
        self.assertEqual(daily["traffic"], 33)
        self.assertEqual(daily["weather"], 39)

    def test_b_reimport_same_source_is_idempotent(self):
        """b. Nhập lại cùng nguồn -> không thay đổi (idempotent, no double count)."""
        import_external_usage(
            self.state_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=True,
        )

        # Second import with identical parameters
        res2 = import_external_usage(
            self.state_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=False,
        )
        self.assertEqual(res2["traffic"]["status"], "already_imported")
        self.assertEqual(res2["weather"]["status"], "already_imported")

        state = State(self.state_file)
        dt_target = datetime.combine(self.target_date, time(12, 0), ZoneInfo(self.tz)).astimezone(timezone.utc)
        daily = state.daily_counts(dt_target, self.tz)
        self.assertEqual(daily["traffic"], 33)
        self.assertEqual(daily["weather"], 39)

    def test_c_ledger_with_runner_attempts_plus_local_source(self):
        """c. Ledger đã có lượt runner + nhập nguồn local -> cộng đúng (e.g. 10 + 33 = 43)."""
        # 1. Initialize state and simulate 10 runner live attempts
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        state = State(self.state_file)
        dt_target = datetime.combine(self.target_date, time(10, 0), ZoneInfo(self.tz)).astimezone(timezone.utc)
        with state.connect() as db:
            for i in range(10):
                db.execute(
                    "INSERT INTO attempts(scope, provider, snapshot, resource, number, reserved_at, outcome) VALUES(?,?,?,?,1,?,'success')",
                    ("runner_scope", "traffic", f"snap_{i}", f"res_{i}", dt_target.isoformat()),
                )

        initial_daily = state.daily_counts(dt_target, self.tz)
        self.assertEqual(initial_daily["traffic"], 10)

        # 2. Import 33 local attempts
        import_external_usage(
            self.state_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=False,
        )

        after_daily = state.daily_counts(dt_target, self.tz)
        # Total must be 10 + 33 = 43, NOT 33!
        self.assertEqual(after_daily["traffic"], 43)
        self.assertEqual(after_daily["weather"], 39)

    def test_d_import_second_external_source(self):
        """d. Nhập thêm nguồn thứ hai -> cộng đúng, không xung đột."""
        import_external_usage(
            self.state_file,
            source_id="source_A",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=True,
        )
        import_external_usage(
            self.state_file,
            source_id="source_B",
            target_date=self.target_date,
            traffic_count=5,
            weather_count=7,
            timezone_name=self.tz,
            allow_init=False,
        )

        state = State(self.state_file)
        dt_target = datetime.combine(self.target_date, time(12, 0), ZoneInfo(self.tz)).astimezone(timezone.utc)
        daily = state.daily_counts(dt_target, self.tz)
        self.assertEqual(daily["traffic"], 38)  # 33 + 5
        self.assertEqual(daily["weather"], 46)  # 39 + 7

    def test_e_checkpoint_and_restore_no_double_counting(self):
        """e. Lặp lại sau khi khôi phục checkpoint -> không cộng trùng."""
        import_external_usage(
            self.state_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=True,
        )

        # Create checkpoint
        metadata = {
            "study_id": "hcm_urban_13paths",
            "registry_version": "main13_20261117_869a0f5a",
            "run_id": "1001",
            "run_attempt": "1",
        }
        archive = create_checkpoint(self.state_file, self.checkpoint_dir, metadata)
        self.assertTrue(archive.is_file())

        # Restore checkpoint into a fresh target path
        restored_file = self.test_dir / "restored" / "capture.sqlite"
        res = restore_checkpoint(
            archive,
            restored_file,
            expected_scope={"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"},
            allow_init=False,
        )
        self.assertEqual(res["status"], "restored")

        # Try to re-import same source into restored state
        import_res = import_external_usage(
            restored_file,
            source_id="local_pilot_20261118_v1",
            target_date=self.target_date,
            traffic_count=33,
            weather_count=39,
            timezone_name=self.tz,
            allow_init=False,
        )
        self.assertEqual(import_res["traffic"]["status"], "already_imported")

        restored_state = State(restored_file)
        dt_target = datetime.combine(self.target_date, time(12, 0), ZoneInfo(self.tz)).astimezone(timezone.utc)
        daily = restored_state.daily_counts(dt_target, self.tz)
        self.assertEqual(daily["traffic"], 33)
        self.assertEqual(daily["weather"], 39)

    def test_f_negative_and_invalid_inputs_rejected(self):
        """Reject negative numbers and invalid dates."""
        with self.assertRaises(ValueError):
            import_external_usage(
                self.state_file,
                source_id="test_src",
                target_date=self.target_date,
                traffic_count=-1,
                weather_count=10,
                allow_init=True,
            )

    def test_g_missing_state_without_allow_init_rejected(self):
        """Missing state database without allow_init must raise FileNotFoundError / RuntimeError."""
        with self.assertRaises(FileNotFoundError):
            import_external_usage(
                self.state_file,
                source_id="test_src",
                target_date=self.target_date,
                traffic_count=10,
                weather_count=10,
                allow_init=False,
            )

        with self.assertRaises(RuntimeError):
            restore_checkpoint(
                self.test_dir / "non_existent_archive.tar.gz",
                self.state_file,
                expected_scope={},
                allow_init=False,
            )


if __name__ == "__main__":
    unittest.main()
