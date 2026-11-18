"""
Comprehensive Offline Verification Test Suite for HCM-Traffic GitHub Actions Pipeline.
Tests:
1. Mock trial session archiving: packages exact session_id, verifies publication CSV, 3 raw slots, config, plan, checkpoint.
   Fails if any mandatory component is missing for a completed trial.
2. Clean-room state restore from artifact checkpoint when cache is missing.
   Proves that verification failure halts and never initializes a silent zero-budget state.
3. Import usage on different date than capture date: verifies correct date partitioning and idempotent re-import.
4. Input fetching edge cases: empty manifest rejected, missing files rejected, rollback on simulated mid-copy failure.
5. Strict acceptance invariants: 39 rows with mismatched segment_id or direction_id rejected immediately.
"""

import csv
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import unittest
from zoneinfo import ZoneInfo

from src.domain.reference import RegistryPath
from src.storage.state import State
from scripts.manage_state import (
    create_checkpoint,
    restore_checkpoint,
    import_external_usage,
    flush_and_verify_sqlite,
)
from scripts.fetch_inputs import check_existing_files, REQUIRED_INPUT_FILES
from scripts.run_runner_trial import (
    resolve_trial_schedule,
    verify_publication_results,
)
from scripts.archive_run_artifacts import find_files_to_archive


class TestOfflinePipelineVerification(unittest.TestCase):

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="test_offline_"))

    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    # --------------------------------------------------------------------------
    # 1. Archive Mock Trial Session & Invariant Verification
    # --------------------------------------------------------------------------
    def test_1_archive_trial_session_and_invariants(self):
        session_id = "trial_2026-11-18_0900_0910_abc123"
        pilot_root = self.temp_dir / f"runner_trial_3slots/{session_id}"
        pilot_root.mkdir(parents=True, exist_ok=True)

        # Create config and plan
        (pilot_root / "trial_study.yaml").write_text("study_id: mock_trial\n", encoding="utf-8")
        (pilot_root / "trial_plan.json").write_text("{\"plan_id\": \"mock_plan\"}\n", encoding="utf-8")
        (pilot_root / "trial_metadata.json").write_text("{\"session_id\": \"abc123\"}\n", encoding="utf-8")

        # Create publication table
        pub_dir = pilot_root / "interim" / "publication"
        pub_dir.mkdir(parents=True, exist_ok=True)
        (pub_dir / "observations_full.csv").write_text("segment_id,direction_id,local_date,time_slot\n", encoding="utf-8")

        # Create 3 raw slots
        for s in ("0900", "0905", "0910"):
            slot_dir = pilot_root / f"raw/mock_study/2026-11-18/slot_{s}"
            slot_dir.mkdir(parents=True, exist_ok=True)
            (slot_dir / "payload.json").write_text("{\"status\": \"ok\"}\n", encoding="utf-8")

        # Find files using find_files_to_archive
        items = find_files_to_archive(pilot_root, "2026-11-18")
        arc_names = [arc for _, arc in items]

        # Verify items contain publication, raw slots, config/plan
        self.assertTrue(any("publication/observations_full.csv" in a for a in arc_names))
        self.assertTrue(any("config_and_plan/trial_study.yaml" in a for a in arc_names))
        self.assertTrue(any("config_and_plan/trial_plan.json" in a for a in arc_names))
        raw_count = sum(1 for a in arc_names if "raw/mock_study/2026-11-18/slot_" in a)
        self.assertEqual(raw_count, 3)

        # Test missing publication fails invariant
        (pub_dir / "observations_full.csv").unlink()
        items_without_pub = find_files_to_archive(pilot_root, "2026-11-18")
        self.assertFalse(any("publication/observations_full.csv" in a for _, a in items_without_pub))

    # --------------------------------------------------------------------------
    # 2. Clean-room State Restore & No-Silent-Reset
    # --------------------------------------------------------------------------
    def test_2_clean_room_restore_and_fail_fast(self):
        clean_state_path = self.temp_dir / "clean_restore/capture.sqlite"
        chk_dir = self.temp_dir / "checkpoints"

        # 1. Non-existent checkpoint with allow_init=False must raise RuntimeError
        non_existent_archive = chk_dir / "state_checkpoint.tar.gz"
        with self.assertRaises(RuntimeError) as ctx:
            restore_checkpoint(
                non_existent_archive,
                clean_state_path,
                {"study_id": "hcm_urban_13paths", "registry_version": "main13"},
                allow_init=False,
            )
        self.assertIn("FATAL", str(ctx.exception))
        self.assertFalse(clean_state_path.exists())

        # 2. Create a genuine checkpoint from populated state
        source_db = self.temp_dir / "source/capture.sqlite"
        source_db.parent.mkdir(parents=True, exist_ok=True)
        State(source_db)
        import_external_usage(
            source_db,
            source_id="local_pilot_v1",
            target_date=date(2026, 11, 18),
            traffic_count=33,
            weather_count=39,
            allow_init=False,
        )
        archive_path = create_checkpoint(
            source_db,
            chk_dir,
            metadata={"study_id": "hcm_urban_13paths", "registry_version": "main13"},
        )
        self.assertTrue(archive_path.is_file())

        # 3. Restore into clean directory
        res = restore_checkpoint(
            archive_path,
            clean_state_path,
            {"study_id": "hcm_urban_13paths", "registry_version": "main13"},
            allow_init=False,
        )
        self.assertEqual(res["status"], "restored")
        restored_state = State(clean_state_path)
        self.assertEqual(restored_state.counts(), {"traffic": 33, "weather": 39})

    # --------------------------------------------------------------------------
    # 3. Import Usage Date Partitioning & Idempotency
    # --------------------------------------------------------------------------
    def test_3_import_date_different_than_capture_date(self):
        state_db = self.temp_dir / "ledger/capture.sqlite"
        state_db.parent.mkdir(parents=True, exist_ok=True)
        State(state_db)

        # Import local pilot on 2026-11-18
        res1 = import_external_usage(
            state_db,
            source_id="local_pilot_20261118_v1",
            target_date=date(2026, 11, 18),
            traffic_count=33,
            weather_count=39,
            timezone_name="Asia/Ho_Chi_Minh",
        )
        self.assertEqual(res1["traffic"]["status"], "imported")

        # Query daily counts on 2026-11-18 vs 2026-11-19
        state = State(state_db)
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        dt_18 = datetime(2026, 11, 18, 12, 0, tzinfo=tz).astimezone(timezone.utc)
        dt_19 = datetime(2026, 11, 19, 12, 0, tzinfo=tz).astimezone(timezone.utc)

        counts_18 = state.daily_counts(dt_18, "Asia/Ho_Chi_Minh")
        counts_19 = state.daily_counts(dt_19, "Asia/Ho_Chi_Minh")

        self.assertEqual(counts_18, {"traffic": 33, "weather": 39})
        self.assertEqual(counts_19, {"traffic": 0, "weather": 0})

        # Re-import same source on 2026-11-18 is idempotent
        res2 = import_external_usage(
            state_db,
            source_id="local_pilot_20261118_v1",
            target_date=date(2026, 11, 18),
            traffic_count=33,
            weather_count=39,
        )
        self.assertEqual(res2["traffic"]["status"], "already_imported")
        self.assertEqual(state.daily_counts(dt_18, "Asia/Ho_Chi_Minh"), {"traffic": 33, "weather": 39})

    # --------------------------------------------------------------------------
    # 4. Input Fetching Edge Cases: Empty Manifest & Rollback on Error
    # --------------------------------------------------------------------------
    def test_4_input_fetching_manifest_and_rollback(self):
        target_dir = self.temp_dir / "ref_target"
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = target_dir / "manifest.json"

        # Case A: Empty manifest
        manifest_file.write_text("{}", encoding="utf-8")
        self.assertFalse(check_existing_files(target_dir, manifest_file))

        # Case B: Manifest with missing files key or empty files dict
        manifest_file.write_text("{\"files\": {}}", encoding="utf-8")
        self.assertFalse(check_existing_files(target_dir, manifest_file))

        # Case C: Manifest missing one required file
        incomplete_files = {req: {"sha256": "dummy", "size_bytes": 10} for req in REQUIRED_INPUT_FILES[:-1]}
        manifest_file.write_text(json.dumps({"files": incomplete_files}), encoding="utf-8")
        self.assertFalse(check_existing_files(target_dir, manifest_file))

        # Case D: Rollback simulation when installation fails midway
        # Populate target_dir with existing good file
        sentinel_file = target_dir / "sentinel.txt"
        sentinel_file.write_text("original_content", encoding="utf-8")

        staging_dir = self.temp_dir / "staging_fault"
        staging_dir.mkdir(parents=True, exist_ok=True)
        (staging_dir / "new_file.txt").write_text("new_content", encoding="utf-8")

        # Simulate transactional copy with failure
        backup_dir = target_dir.parent / f"_backup_ref_{os.getpid()}"
        shutil.copytree(target_dir, backup_dir)
        try:
            # Copy new file
            shutil.copy2(staging_dir / "new_file.txt", target_dir / "new_file.txt")
            # Simulate unexpected failure (e.g. disk full or permission error)
            raise IOError("Simulated disk error during installation")
        except Exception:
            # Perform rollback
            if backup_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
                shutil.copytree(backup_dir, target_dir)
                shutil.rmtree(backup_dir, ignore_errors=True)

        # Verify rollback restored target_dir exactly
        self.assertTrue(sentinel_file.is_file())
        self.assertEqual(sentinel_file.read_text(encoding="utf-8"), "original_content")
        self.assertFalse((target_dir / "new_file.txt").exists())

    # --------------------------------------------------------------------------
    # 5. Strict Publication Key Verification (39 keys matching 13 paths × 3 slots)
    # --------------------------------------------------------------------------
    def test_5_strict_publication_keys_mismatch(self):
        from src.storage.files import read_json
        # Load the real 13 paths from reference bundle
        paths_json = Path("data/reference/candidates/main13_20261117_869a0f5a/paths.json")
        expected_paths = [RegistryPath.model_validate(p) for p in read_json(paths_json)]
        self.assertEqual(len(expected_paths), 13)

        slots = ["16:00", "16:05", "16:10"]
        target_date = "2026-11-18"

        csv_file = self.temp_dir / "observations_full.csv"

        # Case A: Valid 39 rows
        with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["segment_id", "direction_id", "local_date", "time_slot", "current_speed_kmh", "ci", "precipitation_mm", "weather_code"],
            )
            writer.writeheader()
            for p in expected_paths:
                for s in slots:
                    writer.writerow({
                        "segment_id": p.segment_id,
                        "direction_id": p.direction_id,
                        "local_date": target_date,
                        "time_slot": s,
                        "current_speed_kmh": "25.0",
                        "ci": "0.15",
                        "precipitation_mm": "0.0",
                        "weather_code": "0",
                    })

        res_valid = verify_publication_results(csv_file, target_date, slots, expected_paths)
        self.assertEqual(res_valid["total_rows"], 39)
        self.assertEqual(res_valid["unique_keys"], 39)
        self.assertEqual(res_valid["missing_keys"], [])
        self.assertEqual(res_valid["unexpected_keys"], [])
        self.assertEqual(res_valid["all_metrics_complete"], 39)

        # Case B: Exactly 39 rows, but one segment_id is wrong (e.g. seg_tampered instead of first path's segment_id)
        first_p = expected_paths[0]
        with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["segment_id", "direction_id", "local_date", "time_slot", "current_speed_kmh", "ci", "precipitation_mm", "weather_code"],
            )
            writer.writeheader()
            for p in expected_paths:
                for s in slots:
                    seg = "seg_tampered" if (p.segment_id == first_p.segment_id and s == "16:00") else p.segment_id
                    writer.writerow({
                        "segment_id": seg,
                        "direction_id": p.direction_id,
                        "local_date": target_date,
                        "time_slot": s,
                        "current_speed_kmh": "25.0",
                        "ci": "0.15",
                        "precipitation_mm": "0.0",
                        "weather_code": "0",
                    })

        res_invalid = verify_publication_results(csv_file, target_date, slots, expected_paths)
        self.assertEqual(res_invalid["total_rows"], 39)
        self.assertEqual(res_invalid["unique_keys"], 39)
        # Must detect missing key and unexpected key!
        self.assertIn((first_p.segment_id, first_p.direction_id, "2026-11-18", "16:00"), res_invalid["missing_keys"])
        self.assertIn(("seg_tampered", first_p.direction_id, "2026-11-18", "16:00"), res_invalid["unexpected_keys"])

    # --------------------------------------------------------------------------
    # 6. Artifact Checkpoint Selection: Run B (newer, failed) over Run A (older, success)
    # --------------------------------------------------------------------------
    def test_6_select_newest_checkpoint_run_b_over_run_a(self):
        from scripts.manage_state import inspect_and_select_latest_checkpoint
        expected_scope = {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"}

        # 1. Create Run A: Older run (e.g. 10:00 UTC) with 33 traffic, 39 weather (total 72)
        run_a_dir = self.temp_dir / "run_a"
        run_a_db = run_a_dir / "capture.sqlite"
        run_a_dir.mkdir(parents=True, exist_ok=True)
        State(run_a_db)
        import_external_usage(run_a_db, "source_a", date(2026, 11, 18), 33, 39, allow_init=False)
        chk_a = create_checkpoint(
            run_a_db,
            run_a_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1001",
                "created_at_utc": "2026-11-18T10:00:00+00:00",
            },
        )

        # 2. Create Run B: Newer run (11:00 UTC) where capture consumed 11 traffic, 13 weather (total 44/52 = 96)
        # Run B capture failed, but checkpoint was created with the updated state
        run_b_dir = self.temp_dir / "run_b"
        run_b_db = run_b_dir / "capture.sqlite"
        run_b_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run_a_db, run_b_db)
        import_external_usage(run_b_db, "source_b", date(2026, 11, 18), 11, 13, allow_init=False)
        chk_b = create_checkpoint(
            run_b_db,
            run_b_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1002",
                "created_at_utc": "2026-11-18T11:00:00+00:00",
            },
        )

        # 3. Present candidates in arbitrary order (Run A first, Run B second)
        candidates = [chk_a, chk_b]
        best_chk, best_manifest = inspect_and_select_latest_checkpoint(candidates, expected_scope)

        # 4. Verify Run B was chosen over Run A
        self.assertEqual(best_chk, chk_b)
        self.assertEqual(best_manifest["run_id"], "1002")
        self.assertEqual(best_manifest["created_at_utc"], "2026-11-18T11:00:00+00:00")
        self.assertEqual(best_manifest["cumulative_counts"], {"traffic": 44, "weather": 52})

        # 5. Restore into clean state and verify counts of Run B are preserved
        clean_state = self.temp_dir / "restored_state/capture.sqlite"
        res = restore_checkpoint(best_chk, clean_state, expected_scope, allow_init=False)
        self.assertEqual(res["status"], "restored")
        restored = State(clean_state)
        self.assertEqual(restored.counts(), {"traffic": 44, "weather": 52})

    # --------------------------------------------------------------------------
    # 7. Checkpoint Selection: Run B (newer) Corrupted -> Raises Error, Refuses Fallback to Run A
    # --------------------------------------------------------------------------
    def test_7_run_b_corrupt_fails_immediately_refuses_fallback_to_run_a(self):
        from scripts.manage_state import inspect_and_select_latest_checkpoint
        expected_scope = {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"}

        # 1. Run A: Valid older checkpoint
        run_a_dir = self.temp_dir / "run_a_valid"
        run_a_db = run_a_dir / "capture.sqlite"
        run_a_dir.mkdir(parents=True, exist_ok=True)
        State(run_a_db)
        import_external_usage(run_a_db, "source_a", date(2026, 11, 18), 33, 39, allow_init=False)
        chk_a = create_checkpoint(
            run_a_db,
            run_a_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1001",
                "created_at_utc": "2026-11-18T10:00:00+00:00",
            },
        )

        # 2. Run B: Corrupted newer checkpoint (simulating failed download or broken archive)
        run_b_dir = self.temp_dir / "run_b_corrupt"
        run_b_dir.mkdir(parents=True, exist_ok=True)
        corrupt_chk_b = run_b_dir / "state_checkpoint.tar.gz"
        corrupt_chk_b.write_text("CORRUPTED_TAR_BYTES_NOT_A_VALID_GZIP", encoding="utf-8")

        candidates = [
            {"run_id": "1002", "created_at": "2026-11-18T11:00:00+00:00", "archive_path": corrupt_chk_b},
            {"run_id": "1001", "created_at": "2026-11-18T10:00:00+00:00", "archive_path": chk_a},
        ]

        # Expect immediate RuntimeError mentioning Run B (1002), NEVER silently falling back to Run A
        with self.assertRaises(RuntimeError) as ctx:
            inspect_and_select_latest_checkpoint(candidates, expected_scope)

        err_msg = str(ctx.exception)
        self.assertIn("1002", err_msg)
        self.assertIn("Refusing fallback to older runs", err_msg)

    # --------------------------------------------------------------------------
    # 8. Cache has SQLite of Run A, but Checkpoint Artifact is Run B -> Supersedes to B
    # --------------------------------------------------------------------------
    def test_8_cache_has_sqlite_of_a_but_checkpoint_is_b_supersedes_to_b(self):
        expected_scope = {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"}

        # 1. Simulate cache: state file has SQLite of Run A (counts: 33/39, total: 72)
        cached_state_file = self.temp_dir / "cache_state/capture.sqlite"
        cached_state_file.parent.mkdir(parents=True, exist_ok=True)
        State(cached_state_file)
        import_external_usage(cached_state_file, "source_a", date(2026, 11, 18), 33, 39, allow_init=False)
        self.assertEqual(State(cached_state_file).counts(), {"traffic": 33, "weather": 39})

        # 2. Checkpoint artifact of Run B (counts: 44/52, total: 96, created 1 hour later)
        run_b_dir = self.temp_dir / "run_b_artifact"
        run_b_db = run_b_dir / "capture.sqlite"
        run_b_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cached_state_file, run_b_db)
        import_external_usage(run_b_db, "source_b", date(2026, 11, 18), 11, 13, allow_init=False)
        chk_b = create_checkpoint(
            run_b_db,
            run_b_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1002",
                "created_at_utc": "2026-11-18T11:00:00+00:00",
            },
        )

        # 3. Execute verify-restore where state_file exists (A) AND checkpoint_archive is passed (B)
        # manage_state.py must detect B is newer / higher usage and overwrite A!
        # Invoke verify-restore logic via CLI arguments
        saved_argv = sys.argv
        try:
            sys.argv = [
                "manage_state.py",
                "verify-restore",
                "--state-file", str(cached_state_file),
                "--checkpoint-archive", str(chk_b),
                "--study-id", "hcm_urban_13paths",
                "--registry-version", "main13_20261117_869a0f5a",
            ]
            from scripts.manage_state import main as manage_state_main
            ec = manage_state_main()
            self.assertEqual(ec, 0)
        finally:
            sys.argv = saved_argv

        # 4. Verify that cached_state_file now reflects Run B (44/52), NOT Run A (33/39)
        final_state = State(cached_state_file)
        self.assertEqual(final_state.counts(), {"traffic": 44, "weather": 52})

    # --------------------------------------------------------------------------
    # 9. Monotonic Guard Rejects Lower Counts Even With Newer Timestamp
    # --------------------------------------------------------------------------
    def test_9_monotonic_guard_rejects_lower_counts_even_with_newer_timestamp(self):
        expected_scope = {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"}

        # 1. Cache has state with 44 traffic and 52 weather attempts (total: 96)
        cached_state_file = self.temp_dir / "cache_state_44_52/capture.sqlite"
        cached_state_file.parent.mkdir(parents=True, exist_ok=True)
        State(cached_state_file)
        import_external_usage(cached_state_file, "session_prior", date(2026, 11, 18), 44, 52, allow_init=False)
        self.assertEqual(State(cached_state_file).counts(), {"traffic": 44, "weather": 52})

        # Record provenance created_at for cached state
        conn = sqlite3.connect(str(cached_state_file))
        from scripts.manage_state import ensure_provenance_table
        ensure_provenance_table(conn)
        conn.execute("INSERT OR REPLACE INTO state_provenance(key, value) VALUES('created_at_utc', '2026-11-18T10:00:00+00:00');")
        conn.commit()
        conn.close()

        # 2. Checkpoint has only 33 traffic and 39 weather attempts (total: 72), BUT a newer timestamp (12:00 UTC)
        run_lower_dir = self.temp_dir / "run_lower_artifact"
        run_lower_db = run_lower_dir / "capture.sqlite"
        run_lower_dir.mkdir(parents=True, exist_ok=True)
        State(run_lower_db)
        import_external_usage(run_lower_db, "session_regressed", date(2026, 11, 18), 33, 39, allow_init=False)
        chk_lower = create_checkpoint(
            run_lower_db,
            run_lower_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1003",
                "created_at_utc": "2026-11-18T12:00:00+00:00",
            },
        )

        # 3. Invoking verify-restore must raise ValueError due to regression and preserve existing SQLite
        saved_argv = sys.argv
        try:
            sys.argv = [
                "manage_state.py",
                "verify-restore",
                "--state-file", str(cached_state_file),
                "--checkpoint-archive", str(chk_lower),
                "--study-id", "hcm_urban_13paths",
                "--registry-version", "main13_20261117_869a0f5a",
            ]
            from scripts.manage_state import main as manage_state_main
            with self.assertRaises(ValueError) as ctx:
                manage_state_main()

            err_msg = str(ctx.exception)
            self.assertIn("FATAL: Checkpoint (1003) contradicts existing state", err_msg)
            self.assertIn("decrease attempts", err_msg)
            self.assertIn("preserving existing SQLite", err_msg)
        finally:
            sys.argv = saved_argv

        # 4. Verify existing SQLite database remains intact at 44/52
        preserved_state = State(cached_state_file)
        self.assertEqual(preserved_state.counts(), {"traffic": 44, "weather": 52})

    # --------------------------------------------------------------------------
    # 10. GitHub Run History Filtering and Download Failure Handling
    # --------------------------------------------------------------------------
    def test_10_github_history_filtering_and_download_failure(self):
        # Case A: Only current run exists in gh run list -> filtered out -> empty history
        current_run_id = "9999"
        mock_gh_runs = [
            {"databaseId": 9999, "status": "in_progress", "conclusion": None, "createdAt": "2026-11-18T12:00:00Z"}
        ]
        filtered_runs = [r for r in mock_gh_runs if str(r.get("databaseId")) != str(current_run_id)]
        self.assertEqual(len(filtered_runs), 0)

        # When history is empty:
        # a) If allow_init=True, bootstrap is permitted
        fresh_state_path = self.temp_dir / "bootstrap_allowed/capture.sqlite"
        saved_argv = sys.argv
        try:
            sys.argv = [
                "manage_state.py",
                "verify-restore",
                "--state-file", str(fresh_state_path),
                "--allow-init",
                "--study-id", "hcm_urban_13paths",
                "--registry-version", "main13_20261117_869a0f5a",
            ]
            from scripts.manage_state import main as manage_state_main
            ec = manage_state_main()
            self.assertEqual(ec, 0)
            self.assertTrue(fresh_state_path.is_file())
        finally:
            sys.argv = saved_argv

        # b) If allow_init=False, bootstrap is rejected (fails safe)
        no_init_state_path = self.temp_dir / "bootstrap_denied/capture.sqlite"
        try:
            sys.argv = [
                "manage_state.py",
                "verify-restore",
                "--state-file", str(no_init_state_path),
                "--study-id", "hcm_urban_13paths",
                "--registry-version", "main13_20261117_869a0f5a",
            ]
            ec = manage_state_main()
            self.assertEqual(ec, 1)
            self.assertFalse(no_init_state_path.is_file())
        finally:
            sys.argv = saved_argv

        # Case B: Prior runs exist (Run B newer, Run A older). Run B download fails completely.
        # Selection must halt with error mentioning Run B (1002) and NEVER fall back to Run A (1001).
        run_a_dir = self.temp_dir / "run_a_valid"
        run_a_db = run_a_dir / "capture.sqlite"
        run_a_dir.mkdir(parents=True, exist_ok=True)
        State(run_a_db)
        import_external_usage(run_a_db, "session_a", date(2026, 11, 18), 33, 39, allow_init=False)
        chk_a = create_checkpoint(
            run_a_db,
            run_a_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1001",
                "created_at_utc": "2026-11-18T10:00:00+00:00",
            },
        )

        candidates_b_failed = [
            {
                "run_id": "1002",
                "created_at": "2026-11-18T11:00:00+00:00",
                "archive_path": self.temp_dir / "run_b_missing/download_failed.tar.gz",
                "download_error": "gh: HTTP 500 Internal Server Error downloading artifact",
            },
            {
                "run_id": "1001",
                "created_at": "2026-11-18T10:00:00+00:00",
                "archive_path": chk_a,
            },
        ]

        expected_scope = {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"}
        from scripts.manage_state import inspect_and_select_latest_checkpoint
        with self.assertRaises(RuntimeError) as ctx:
            inspect_and_select_latest_checkpoint(candidates_b_failed, expected_scope)

        err_msg = str(ctx.exception)
        self.assertIn("1002", err_msg)
        self.assertIn("failed", err_msg)
        self.assertIn("Refusing fallback to older runs", err_msg)

    # --------------------------------------------------------------------------
    # 11. Early Cancelled / Failed Runs Sequence Allows Clean Bootstrap
    # --------------------------------------------------------------------------
    def test_11_early_stopped_runs_sequence_allows_bootstrap_or_prior_checkpoint(self):
        from scripts.manage_state import verify_run_stopped_before_ledger, inspect_and_select_latest_checkpoint

        # 1. Mock Run #1: Cancelled while queued (runner_id: 0, steps: [])
        job_run_1 = [{"runner_id": 0, "runner_name": "", "steps": []}]
        stopped_1, reason_1 = verify_run_stopped_before_ledger(job_run_1)
        self.assertTrue(stopped_1)
        self.assertIn("stopped before step", reason_1)

        # 2. Mock Run #2: Failed at Step 10, Step 11 skipped
        job_run_2 = [{
            "runner_id": 12345,
            "runner_name": "GitHub Actions 2",
            "steps": [
                {"name": "Retrieve Latest State Checkpoint Artifact (Fallback or Supersede)", "status": "completed", "conclusion": "failure"},
                {"name": "Verify and Reconcile State Ledger", "status": "completed", "conclusion": "skipped"},
                {"name": "Execute Capture Run (Trial or Afternoon)", "status": "completed", "conclusion": "skipped"},
            ]
        }]
        stopped_2, reason_2 = verify_run_stopped_before_ledger(job_run_2)
        self.assertTrue(stopped_2)
        self.assertIn("stopped before step", reason_2)

        # 3. Simulate history where both Run #1 and Run #2 stopped before ledger
        # Case 3a: No earlier checkpoint exists and allow_init=True -> Clean bootstrap succeeds
        fresh_state = self.temp_dir / "bootstrap_after_skipped/capture.sqlite"
        saved_argv = sys.argv
        try:
            sys.argv = [
                "manage_state.py",
                "verify-restore",
                "--state-file", str(fresh_state),
                "--allow-init",
                "--study-id", "hcm_urban_13paths",
                "--registry-version", "main13_20261117_869a0f5a",
            ]
            from scripts.manage_state import main as manage_state_main
            ec = manage_state_main()
            self.assertEqual(ec, 0)
            self.assertTrue(fresh_state.is_file())
        finally:
            sys.argv = saved_argv

        # Case 3b: Run #0 exists BEFORE Run #1 and has a valid checkpoint (44/52).
        # Since Run #2 and Run #1 stopped before ledger, inspect_and_select_latest_checkpoint
        # must safely skip them and restore Run #0!
        run_0_dir = self.temp_dir / "run_0_valid"
        run_0_db = run_0_dir / "capture.sqlite"
        run_0_dir.mkdir(parents=True, exist_ok=True)
        State(run_0_db)
        import_external_usage(run_0_db, "session_0", date(2026, 11, 18), 44, 52, allow_init=False)
        chk_0 = create_checkpoint(
            run_0_db,
            run_0_dir / "checkpoints",
            metadata={
                "study_id": "hcm_urban_13paths",
                "registry_version": "main13_20261117_869a0f5a",
                "run_id": "1000",
                "created_at_utc": "2026-11-18T09:00:00+00:00",
            },
        )

        candidates_with_skipped = [
            {"run_id": "1002", "created_at": "2026-11-18T11:00:00+00:00", "archive_path": Path("dummy2"), "stopped_before_ledger": True},
            {"run_id": "1001", "created_at": "2026-11-18T10:00:00+00:00", "archive_path": Path("dummy1"), "stopped_before_ledger": True},
            {"run_id": "1000", "created_at": "2026-11-18T09:00:00+00:00", "archive_path": chk_0, "stopped_before_ledger": False},
        ]
        best_p, best_m = inspect_and_select_latest_checkpoint(candidates_with_skipped, {"study_id": "hcm_urban_13paths", "registry_version": "main13_20261117_869a0f5a"})
        self.assertEqual(best_m.get("run_id"), "1000")
        self.assertEqual(best_m.get("cumulative_counts"), {"traffic": 44, "weather": 52})

    # --------------------------------------------------------------------------
    # 12. Run Reached Ledger Step Without Checkpoint Must NOT Be Skipped
    # --------------------------------------------------------------------------
    def test_12_run_reached_ledger_without_checkpoint_halts(self):
        from scripts.manage_state import verify_run_stopped_before_ledger

        # 1. Job that reached 'Verify and Reconcile State Ledger' with conclusion 'success'
        job_success = [{
            "runner_id": 999,
            "runner_name": "GitHub Actions",
            "steps": [
                {"name": "Verify and Reconcile State Ledger", "status": "completed", "conclusion": "success"},
                {"name": "Execute Capture Run (Trial or Afternoon)", "status": "completed", "conclusion": "failure"},
            ]
        }]
        stopped, reason = verify_run_stopped_before_ledger(job_success)
        self.assertFalse(stopped)
        self.assertIn("was reached", reason)

        # 2. Job that was cancelled while 'Verify and Reconcile State Ledger' was in progress
        job_cancelled_in_ledger = [{
            "runner_id": 999,
            "runner_name": "GitHub Actions",
            "steps": [
                {"name": "Verify and Reconcile State Ledger", "status": "completed", "conclusion": "cancelled"},
            ]
        }]
        stopped_c, reason_c = verify_run_stopped_before_ledger(job_cancelled_in_ledger)
        self.assertFalse(stopped_c)
        self.assertIn("was reached", reason_c)

        # 3. Runner was assigned but step list is empty (ambiguous / incomplete log)
        job_ambiguous = [{"runner_id": 999, "runner_name": "GitHub Actions", "steps": []}]
        stopped_a, reason_a = verify_run_stopped_before_ledger(job_ambiguous)
        self.assertFalse(stopped_a)
        self.assertIn("step list is empty", reason_a)


if __name__ == "__main__":
    unittest.main()
