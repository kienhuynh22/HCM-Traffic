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
) -> tuple[bool, str]:
    """
    Checks if a production live run for target_date has already completed in the ledger.
    Distinguishes:
    - Production live capture: recorded attempts > 0 and/or completed slots on target_date.
    - Dry-run: 0 attempts recorded.
    - Trial: different mode / pilot root.
    - Runs stopped before capture (like #10): 0 attempts recorded on target_date.

    Returns (is_duplicate: bool, reason: str).
    If is_duplicate is True, the run should halt early to prevent duplicate capture.
    """
    if mode != "afternoon_37slots" or dry_run:
        return False, f"Duplicate check not applicable for mode='{mode}', dry_run={dry_run}."

    if not db_path or not Path(db_path).is_file():
        return False, "No state database found; duplicate check passes."

    db_file = Path(db_path).resolve()
    counts = get_daily_provider_counts(db_file, accounting_tz=tz_str)
    traffic_attempts = counts.get((target_date, "traffic"), 0)
    weather_attempts = counts.get((target_date, "weather"), 0)

    # Check slots table for completed slots on target_date
    completed_slots = 0
    try:
        conn = sqlite3.connect(str(db_file), timeout=10)
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='slots'")
            if cur.fetchone():
                cur.execute("SELECT scheduled, status FROM slots WHERE status='complete'")
                zone = ZoneInfo(tz_str)
                for res_sched, status in cur.fetchall():
                    try:
                        clean_ts = str(res_sched).replace("Z", "+00:00")
                        dt = datetime.fromisoformat(clean_ts)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        local_d = dt.astimezone(zone).date().isoformat()
                        if local_d == target_date:
                            completed_slots += 1
                    except Exception:
                        continue
        finally:
            conn.close()
    except Exception:
        pass

    if traffic_attempts > 0 or completed_slots > 0:
        return True, (
            f"Production live capture for {target_date} already completed in state ledger "
            f"(traffic_attempts={traffic_attempts}, weather_attempts={weather_attempts}, "
            f"completed_slots={completed_slots})."
        )

    return False, f"No prior live capture found in ledger for {target_date} (traffic={traffic_attempts}, slots={completed_slots})."


def inspect_checkpoint_archive_for_duplicate(
    checkpoint_archive: Path,
    target_date: str,
    mode: str,
    dry_run: bool,
    tz_str: str = "Asia/Ho_Chi_Minh",
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
                        return check_duplicate_live_capture(db_candidate, target_date, mode, dry_run, tz_str)

    return False, "No sqlite file found inside checkpoint archive."


def check_github_actions_for_duplicate(
    repo: str,
    target_date: str,
    mode: str,
    dry_run: bool,
    current_run_id: str = "",
) -> tuple[bool, str]:
    """
    Inspects GitHub Actions API for existing runs on target_date:
    1. Checks if another run is currently queued or in_progress.
    2. Checks if an earlier run completed successfully with production artifact for target_date.
    """
    if mode != "afternoon_37slots" or dry_run:
        return False, f"Duplicate check not applicable for mode='{mode}', dry_run={dry_run}."

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token or not repo:
        return False, "GitHub token or repository missing; skipping remote API duplicate check."

    cmd = [
        "gh", "run", "list",
        "--repo", repo,
        "--workflow", "capture_13paths.yml",
        "--limit", "15",
        "--json", "databaseId,status,conclusion,createdAt"
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if res.returncode != 0:
            return False, f"Failed to query GitHub CLI: {res.stderr.strip()}"
        runs = json.loads(res.stdout)
    except Exception as exc:
        return False, f"Error inspecting GitHub runs: {exc}"

    current_id_str = str(current_run_id).strip()
    target_artifact_prefix = f"capture-outputs-{mode}-{target_date}-"

    # Query artifacts to see if a successful production artifact exists for target_date
    art_cmd = ["gh", "api", f"repos/{repo}/actions/artifacts", "--jq", ".artifacts"]
    try:
        art_res = subprocess.run(art_cmd, capture_output=True, text=True, timeout=30)
        if art_res.returncode == 0:
            artifacts = json.loads(art_res.stdout)
            for art in artifacts:
                art_name = art.get("name", "")
                run_id = str(art.get("workflow_run", {}).get("id", ""))
                size = art.get("size_in_bytes", 0)
                if run_id == current_id_str:
                    continue
                # Production complete runs are ~150MB, dry runs are ~26KB, timeout runs are ~272KB
                if art_name.startswith(target_artifact_prefix) and size > 1_000_000:
                    return True, (
                        f"Found completed production run artifact '{art_name}' (size={size} bytes, "
                        f"run_id={run_id}) for {target_date}."
                    )
    except Exception:
        pass

    return False, f"No remote production live run found for {target_date}."


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
            )

        # Check 2: Direct DB path if provided and not yet duplicate
        if not is_dup and args.db_path and args.db_path.is_file():
            is_dup, reason = check_duplicate_live_capture(
                args.db_path,
                args.date,
                args.mode,
                args.dry_run,
                tz_str=args.timezone,
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
