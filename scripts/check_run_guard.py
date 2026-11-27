"""
Guard module for HCM-Traffic capture runs.
Validates startup launch window and checks for same-day duplicate live capture.
Enforces that automated afternoon runs start strictly between 15:30 and before 16:00 Vietnam time.
Detects prior live runs on the same date to avoid redundant calls, preserving budget and state.
"""

import argparse
from datetime import datetime, time, timezone
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tarfile
from zoneinfo import ZoneInfo

from scripts.manage_state import get_daily_provider_counts


def check_startup_window(
    event_name: str,
    mode: str,
    is_automated: bool,
    now_dt: datetime | None = None,
    tz_str: str = "Asia/Ho_Chi_Minh",
) -> tuple[bool, str]:
    """
    Checks if an automated afternoon run is within the valid launch window:
    15:30 to before 16:00 in Vietnam time.
    Returns (is_valid: bool, reason: str).
    If is_valid is False, the run should halt early.
    """
    if mode != "afternoon_37slots":
        return True, f"Mode '{mode}' does not enforce afternoon startup window."

    if not is_automated:
        return True, "Manual run without automated flag; startup window check skipped."

    tz = ZoneInfo(tz_str)
    current_vn = (now_dt or datetime.now(timezone.utc)).astimezone(tz)
    cur_time = current_vn.time()

    window_start = time(15, 30)
    window_end = time(16, 0)

    if window_start <= cur_time < window_end:
        return True, f"Started within valid launch window [15:30, 16:00) ICT at {current_vn.strftime('%H:%M:%S')}."
    else:
        return False, (
            f"Automated run started outside startup window [15:30, 16:00) {tz_str} "
            f"(started at {current_vn.strftime('%H:%M:%S')} ICT on {current_vn.strftime('%Y-%m-%d')})."
        )


def check_duplicate_live_capture(
    db_path: Path | None,
    target_date: str,
    mode: str,
    dry_run: bool,
    tz_str: str = "Asia/Ho_Chi_Minh",
    study_id: str = "hcm_urban_13paths",
) -> tuple[bool, str]:
    """
    Checks if a production live run for target_date has already captured (fully or partially)
    in the state ledger.
    Distinguishes:
    - Production live capture (even partial, e.g. 36/37 slots in Run #20 or 1/37):
      slots with study == study_id and status in ('complete', 'partial', 'interrupted', 'collecting')
      on target_date, or attempts linked to those production slots.
    - Dry-run: 0 slots and 0 attempts recorded.
    - Trial (trial_3slots): slots have study starting with 'hcm_urban_trial_', NOT study_id.
    - Runs stopped before capture (like #10 or #21): 0 slots and 0 attempts recorded on target_date.

    Returns (is_duplicate: bool, reason: str).
    If is_duplicate is True, the run should halt early to prevent duplicate capture.
    """
    if mode != "afternoon_37slots" or dry_run:
        return False, f"Duplicate check not applicable for mode='{mode}', dry_run={dry_run}."

    if not db_path or not Path(db_path).is_file():
        return False, "No state database found; duplicate check passes."

    db_file = Path(db_path).resolve()
    zone = ZoneInfo(tz_str)
    captured_slots = 0
    missed_slots = 0
    prod_snapshots: set[str] = set()
    prod_attempts = 0
    prod_traffic_attempts = 0
    prod_weather_attempts = 0

    conn = sqlite3.connect(str(db_file), timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='slots'")
        if cur.fetchone():
            cur.execute(
                "SELECT scheduled, snapshot, status FROM slots WHERE study=?",
                (study_id,),
            )
            for res_sched, snap_id, status in cur.fetchall():
                try:
                    clean_ts = str(res_sched).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(clean_ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    local_d = dt.astimezone(zone).date().isoformat()
                    if local_d == target_date:
                        if snap_id:
                            prod_snapshots.add(str(snap_id))
                        if status in ("complete", "partial", "interrupted", "collecting"):
                            captured_slots += 1
                        elif status == "missed":
                            missed_slots += 1
                except Exception:
                    continue

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='attempts'")
        if cur.fetchone() and prod_snapshots:
            placeholders = ",".join("?" for _ in prod_snapshots)
            cur.execute(
                f"SELECT provider, reserved_at FROM attempts WHERE snapshot IN ({placeholders})",
                tuple(prod_snapshots),
            )
            for prov, res_at in cur.fetchall():
                try:
                    clean_ts = str(res_at).replace("Z", "+00:00")
                    if " " in clean_ts and "T" not in clean_ts:
                        clean_ts = clean_ts.replace(" ", "T")
                    dt = datetime.fromisoformat(clean_ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    local_d = dt.astimezone(zone).date().isoformat()
                    if local_d == target_date:
                        prod_attempts += 1
                        if prov == "traffic":
                            prod_traffic_attempts += 1
                        elif prov == "weather":
                            prod_weather_attempts += 1
                except Exception:
                    continue
    finally:
        conn.close()

    if captured_slots > 0 or prod_attempts > 0:
        return True, (
            f"Production live capture for {target_date} (study={study_id}) already occurred in state ledger "
            f"(captured_slots={captured_slots}, missed_slots={missed_slots}, "
            f"traffic_attempts={prod_traffic_attempts}, weather_attempts={prod_weather_attempts})."
        )

    return False, (
        f"No prior production live capture found in ledger for {target_date} "
        f"(study={study_id}, captured_slots={captured_slots}, prod_attempts={prod_attempts})."
    )


def inspect_checkpoint_archive_for_duplicate(
    checkpoint_archive: Path,
    target_date: str,
    mode: str,
    dry_run: bool,
    tz_str: str = "Asia/Ho_Chi_Minh",
    study_id: str = "hcm_urban_13paths",
) -> tuple[bool, str]:
    """
    Extracts sqlite db from checkpoint tar archive to a temporary directory
    and checks if target_date has already completed live capture.
    """
    if mode != "afternoon_37slots" or dry_run:
        return False, f"Duplicate check not applicable for mode='{mode}', dry_run={dry_run}."

    if not checkpoint_archive or not Path(checkpoint_archive).is_file():
        return False, "Checkpoint archive does not exist; duplicate check passes."

    import tempfile
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        with tarfile.open(str(checkpoint_archive), "r:*") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".sqlite") and not member.name.startswith("."):
                    tar.extract(member, path=tmp_path)
                    db_candidate = tmp_path / member.name
                    if db_candidate.is_file():
                        return check_duplicate_live_capture(
                            db_candidate, target_date, mode, dry_run, tz_str, study_id=study_id
                        )

    return False, "No sqlite file found inside checkpoint archive."


def check_github_actions_for_duplicate(
    repo: str,
    target_date: str,
    mode: str,
    dry_run: bool,
    current_run_id: str = "",
) -> tuple[bool, str]:
    """
    Inspects GitHub Actions API for existing production live runs on target_date.
    Distinguishes full/partial production captures (even if GitHub conclusion == 'failure'
    like Run #20 with 36/37 slots) from dry-runs, trials, and runs stopped before capture (#10, #21).
    """
    if mode != "afternoon_37slots" or dry_run:
        return False, f"Duplicate check not applicable for mode='{mode}', dry_run={dry_run}."

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token or not repo:
        return False, "GitHub token or repository missing; skipping remote API duplicate check."

    current_id_str = str(current_run_id).strip()
    target_artifact_prefix = f"capture-outputs-{mode}-{target_date}-"
    zone = ZoneInfo("Asia/Ho_Chi_Minh")
    slot1_utc = datetime.fromisoformat(f"{target_date}T16:00:00+07:00").astimezone(timezone.utc)

    art_cmd = ["gh", "api", f"repos/{repo}/actions/artifacts?per_page=100", "--jq", ".artifacts"]
    try:
        art_res = subprocess.run(art_cmd, capture_output=True, text=True, timeout=30)
        if art_res.returncode != 0:
            return False, f"Failed to query GitHub artifacts: {art_res.stderr.strip()}"
        artifacts = json.loads(art_res.stdout)
        for art in artifacts:
            art_name = art.get("name", "")
            run_id = str(art.get("workflow_run", {}).get("id", ""))
            size = art.get("size_in_bytes", 0)
            if run_id == current_id_str or not art_name.startswith(target_artifact_prefix):
                continue

            # Verify run metadata and job steps so we don't confuse dry-runs or 0-slot timeouts
            run_cmd = ["gh", "api", f"repos/{repo}/actions/runs/{run_id}"]
            jobs_cmd = ["gh", "api", f"repos/{repo}/actions/runs/{run_id}/jobs", "--jq", ".jobs"]
            r_res = subprocess.run(run_cmd, capture_output=True, text=True, timeout=30)
            j_res = subprocess.run(jobs_cmd, capture_output=True, text=True, timeout=30)
            if r_res.returncode != 0 or j_res.returncode != 0:
                continue

            run_info = json.loads(r_res.stdout)
            title = str(run_info.get("display_title", ""))
            if "dry-run" in title or "[trial_3slots" in title:
                continue

            jobs = json.loads(j_res.stdout)
            for job in jobs:
                for step in job.get("steps", []):
                    if step.get("name") == "Execute Capture Run (Trial or Afternoon)":
                        conc = step.get("conclusion")
                        comp_at = step.get("completed_at")
                        start_at = step.get("started_at")
                        comp_dt = datetime.fromisoformat(comp_at.replace("Z", "+00:00")) if comp_at else None
                        start_dt = datetime.fromisoformat(start_at.replace("Z", "+00:00")) if start_at else None
                        dur_s = (comp_dt - start_dt).total_seconds() if (comp_dt and start_dt) else 0

                        if conc == "skipped":
                            continue
                        if conc == "cancelled" and comp_dt and comp_dt < slot1_utc:
                            # Timed out before 16:00 VN slot 1 (like #10 and #21) -> 0 slots captured
                            continue
                        if conc == "success" and (size >= 2_000_000 or (comp_dt and comp_dt >= slot1_utc and dur_s >= 120)):
                            return True, (
                                f"Found completed/partial production run artifact '{art_name}' "
                                f"(run_id={run_id}, run_conclusion={run_info.get('conclusion')}, size={size} bytes)."
                            )
                        if conc == "cancelled" and comp_dt and comp_dt >= slot1_utc and size >= 2_000_000:
                            return True, (
                                f"Found partial afternoon production run artifact '{art_name}' "
                                f"(run_id={run_id}, size={size} bytes, cancelled after 16:00 ICT)."
                            )
    except Exception as exc:
        return False, f"Error inspecting GitHub artifacts/runs: {exc}"

    return False, f"No remote production live capture found for {target_date}."


def main():
    parser = argparse.ArgumentParser(description="Check startup window and duplicate live capture.")
    subparsers = parser.add_subparsers(dest="command")

    # startup-window subcommand
    sw_parser = subparsers.add_parser("startup-window")
    sw_parser.add_argument("--event", required=True, help="GitHub event name")
    sw_parser.add_argument("--mode", required=True, help="Run mode")
    sw_parser.add_argument("--automated", action="store_true", help="Whether run is marked as automated")
    sw_parser.add_argument("--timezone", default="Asia/Ho_Chi_Minh")

    # check-duplicate subcommand
    dup_parser = subparsers.add_parser("check-duplicate")
    dup_parser.add_argument("--date", required=True, help="Target accounting date YYYY-MM-DD")
    dup_parser.add_argument("--mode", required=True, help="Run mode")
    dup_parser.add_argument("--dry-run", action="store_true", help="Whether dry run")
    dup_parser.add_argument("--db-path", type=Path, default=None)
    dup_parser.add_argument("--checkpoint-archive", type=Path, default=None)
    dup_parser.add_argument("--repo", default="")
    dup_parser.add_argument("--current-run-id", default="")
    dup_parser.add_argument("--timezone", default="Asia/Ho_Chi_Minh")
    dup_parser.add_argument("--study-id", default="hcm_urban_13paths")

    args = parser.parse_args()

    if args.command == "startup-window":
        is_valid, reason = check_startup_window(
            args.event,
            args.mode,
            args.automated,
            tz_str=args.timezone,
        )
        print(f"Startup window check: valid={is_valid}, reason={reason}")
        if not is_valid:
            sys.exit(2)
        sys.exit(0)

    elif args.command == "check-duplicate":
        is_dup = False
        reason = ""

        # Check 1: Checkpoint archive if provided
        if args.checkpoint_archive and args.checkpoint_archive.is_file():
            is_dup, reason = inspect_checkpoint_archive_for_duplicate(
                args.checkpoint_archive,
                args.date,
                args.mode,
                args.dry_run,
                tz_str=args.timezone,
                study_id=args.study_id,
            )

        # Check 2: Direct DB path if provided and not yet duplicate
        if not is_dup and args.db_path and args.db_path.is_file():
            is_dup, reason = check_duplicate_live_capture(
                args.db_path,
                args.date,
                args.mode,
                args.dry_run,
                tz_str=args.timezone,
                study_id=args.study_id,
            )

        # Check 3: GitHub API if repo provided and not yet duplicate
        if not is_dup and args.repo:
            is_dup, reason = check_github_actions_for_duplicate(
                args.repo,
                args.date,
                args.mode,
                args.dry_run,
                current_run_id=args.current_run_id,
            )

        print(f"Duplicate check: duplicate={is_dup}, reason={reason}")
        if is_dup:
            sys.exit(3)
        sys.exit(0)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
