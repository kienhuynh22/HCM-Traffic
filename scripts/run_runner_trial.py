"""
Runner Trial Orchestrator for HCM-Traffic.
Executes an isolated 3-slot capture trial on GitHub Actions runner.
Guarantees:
1. Inputs and graph are loaded BEFORE computing schedule.
2. Schedule calculation rounds forward and shifts midnight crossings cleanly.
3. Strict timing margin check before capture: aborts if setup eats margin (< 60s).
4. Uses canonical shared state ledger: properly accounts for consumed quota.
5. Strict acceptance criteria: complete=3, missed=0, partial=0, interrupted=0, 39 unique keys.
"""

import argparse
import csv
from datetime import datetime, date, time, timedelta, timezone
import json
from pathlib import Path
import sys
import time as time_mod
import uuid
from zoneinfo import ZoneInfo

import os
import yaml

from src.config import load_config, ResolvedConfig
from src.reference.graph import load_graph
from src.preparation.plan import build_capture_plan
from src.storage.files import read_json, write_json
from src.domain.reference import RegistryPath
from src.capture.preflight import preflight
from src.storage.state import State
from src.capture.scheduler import Clock
from src.workflow import run_day


def resolve_trial_schedule(
    now_vn: datetime,
    delay_minutes: int,
    user_date_str: str = "",
    user_slots_str: str = "",
) -> tuple[date, list[str], list[datetime]]:
    """
    Resolves the 3 target trial slots and accounting date.
    - If user provides explicit slots (HH:MM,HH:MM,HH:MM):
      Verifies strictly 3 ascending slots. If --date is provided, combines them.
      If first slot is in the past, raises ValueError.
    - If user provides --date without slots:
      If date is in the past: raises ValueError.
      If date is in the future: schedules 3 slots starting at 00:15 on that future date.
      If date is today: calculates slots from current time + delay_minutes.
        If slots cross midnight into tomorrow: raises ValueError explaining today cannot fit 3 slots before midnight.
    - If user provides neither --date nor slots:
      Computes future slots from current time + delay_minutes, automatically shifting to tomorrow 00:15 if midnight is crossed.
    """
    if user_slots_str:
        explicit_slots = [s.strip() for s in user_slots_str.split(",") if s.strip()]
        if len(explicit_slots) != 3:
            raise ValueError(f"Explicit trial slots must contain exactly 3 slots, got: {explicit_slots}")
        target_date = date.fromisoformat(user_date_str) if user_date_str else now_vn.date()
        slots_dt = [
            datetime.combine(target_date, time.fromisoformat(s), now_vn.tzinfo)
            for s in explicit_slots
        ]
        if not (slots_dt[0] < slots_dt[1] < slots_dt[2]):
            raise ValueError(f"Slots must be strictly ascending: {explicit_slots}")
        if slots_dt[0] <= now_vn:
            raise ValueError(f"Slot 1 ({slots_dt[0].strftime('%Y-%m-%d %H:%M')}) is not in the future relative to current time ({now_vn.strftime('%Y-%m-%d %H:%M')})")
        return target_date, explicit_slots, slots_dt

    if user_date_str:
        requested_date = date.fromisoformat(user_date_str)
        if requested_date < now_vn.date():
            raise ValueError(f"Cannot schedule trial for past date: {requested_date} < today {now_vn.date()}")
        elif requested_date > now_vn.date():
            target_start = datetime.combine(requested_date, time(0, 15), now_vn.tzinfo)
            slots_dt = [target_start + timedelta(minutes=5 * i) for i in range(3)]
            slots = [dt.strftime("%H:%M") for dt in slots_dt]
            return requested_date, slots, slots_dt
        else:
            # Requested date is today
            target_start = now_vn + timedelta(minutes=max(5, delay_minutes))
            remainder = target_start.minute % 5
            if remainder != 0:
                target_start += timedelta(minutes=(5 - remainder))
            target_start = target_start.replace(second=0, microsecond=0)
            slot_3_time = target_start + timedelta(minutes=10)
            if slot_3_time.date() != requested_date:
                raise ValueError(
                    f"Requested date {requested_date} cannot accommodate 3 future slots before midnight! "
                    f"Current time is {now_vn.strftime('%H:%M:%S')}. "
                    "Omit --date to automatically shift to tomorrow 00:15, or provide explicit future slots."
                )
            slots_dt = [target_start + timedelta(minutes=5 * i) for i in range(3)]
            slots = [dt.strftime("%H:%M") for dt in slots_dt]
            return requested_date, slots, slots_dt

    # Automatic mode without explicit date: auto-shifts across midnight if needed
    target_start = now_vn + timedelta(minutes=max(5, delay_minutes))
    remainder = target_start.minute % 5
    if remainder != 0:
        target_start += timedelta(minutes=(5 - remainder))
    target_start = target_start.replace(second=0, microsecond=0)
    slot_3_time = target_start + timedelta(minutes=10)
    if slot_3_time.date() != target_start.date():
        tomorrow = target_start.date() + timedelta(days=1)
        target_start = datetime.combine(tomorrow, time(0, 15), now_vn.tzinfo)

    slots_dt = [target_start + timedelta(minutes=5 * i) for i in range(3)]
    slots = [dt.strftime("%H:%M") for dt in slots_dt]
    return slots_dt[0].date(), slots, slots_dt


def compute_future_slots(now_vn: datetime, delay_minutes: int) -> tuple[date, list[str], list[datetime]]:
    """Backward-compatible helper for automatic future slot computation."""
    return resolve_trial_schedule(now_vn, delay_minutes)


def verify_publication_results(
    csv_path: Path,
    target_date_str: str,
    slots: list[str],
    expected_paths: list[RegistryPath],
) -> dict:
    """
    Verifies that publication table contains exactly 39 unique rows matching the 13 paths × 3 slots.
    Raises ValueError if rows are missing, keys do not match expected paths, or duplicates exist.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"Publication file not found at {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    trial_rows = [
        r for r in rows
        if r.get("local_date") == target_date_str and r.get("time_slot") in slots
    ]

    expected_keys = {
        (p.segment_id, p.direction_id, target_date_str, s)
        for p in expected_paths
        for s in slots
    }

    observed_keys = {
        (r.get("segment_id"), r.get("direction_id"), r.get("local_date"), r.get("time_slot"))
        for r in trial_rows
    }

    missing_keys = expected_keys - observed_keys
    unexpected_keys = observed_keys - expected_keys

    speed_nn = sum(r.get("current_speed_kmh") not in (None, "") for r in trial_rows)
    ci_nn = sum(r.get("ci") not in (None, "") for r in trial_rows)
    weather_comp = sum(
        r.get("precipitation_mm") not in (None, "") and r.get("weather_code") not in (None, "")
        for r in trial_rows
    )
    all_metrics_comp = sum(
        r.get("current_speed_kmh") not in (None, "")
        and r.get("ci") not in (None, "")
        and r.get("precipitation_mm") not in (None, "")
        and r.get("weather_code") not in (None, "")
        for r in trial_rows
    )

    return {
        "total_rows": len(trial_rows),
        "unique_keys": len(observed_keys),
        "expected_keys_count": len(expected_keys),
        "has_duplicates": len(observed_keys) != len(trial_rows),
        "missing_keys": sorted(list(missing_keys)),
        "unexpected_keys": sorted(list(unexpected_keys)),
        "speed_complete": speed_nn,
        "ci_complete": ci_nn,
        "weather_complete": weather_comp,
        "all_metrics_complete": all_metrics_comp,
        "slots_found": sorted(set(r.get("time_slot") for r in trial_rows)),
    }


def main():
    parser = argparse.ArgumentParser(description="HCM-Traffic 3-slot trial runner.")
    parser.add_argument("--date", type=str, default="", help="Capture date YYYY-MM-DD")
    parser.add_argument("--slots", type=str, default="", help="Comma-separated slots HH:MM,HH:MM,HH:MM")
    parser.add_argument("--delay-minutes", type=int, default=12, help="Safety margin in minutes before slot 1")
    parser.add_argument("--dry-run", action="store_true", help="Run preflight check only without live collection")
    parser.add_argument("--allow-online", action="store_true", help="Allow online network access")
    parser.add_argument("--base-config", type=Path, default=Path("config/ci/study_13paths.yaml"))
    parser.add_argument("--state-file", type=Path, default=None, help="Path to shared capture.sqlite state ledger")
    args = parser.parse_args()

    t_start = time_mod.time()
    tz = ZoneInfo("Asia/Ho_Chi_Minh")

    # 1. Load inputs and graph FIRST before calculating time slots
    print("=" * 80)
    print("STEP 1: Loading graph and reference bundle into memory...")
    base_cfg = load_config(args.base_config.resolve())
    graph = load_graph(base_cfg.study.graph_cache)
    paths = [RegistryPath.model_validate(p) for p in read_json(base_cfg.study.selected_paths_file)]
    load_elapsed = time_mod.time() - t_start
    print(f"Graph loaded ({len(paths)} paths, {len(graph.nodes)} nodes, {len(graph.edges)} edges) in {load_elapsed:.1f}s.")

    # 2. Determine target slots with safety delay margin AFTER graph is loaded
    now_vn = datetime.now(tz)
    target_date, slots, slots_dt = resolve_trial_schedule(
        now_vn=now_vn,
        delay_minutes=args.delay_minutes,
        user_date_str=args.date,
        user_slots_str=args.slots,
    )

    date_str = target_date.isoformat()
    session_id = f"trial_{date_str}_{slots[0].replace(':', '')}_{slots[-1].replace(':', '')}_{uuid.uuid4().hex[:6]}"

    print("\nSTEP 2: Schedule & Provenance Definition")
    print(f"  Target Date:  {date_str} (Asia/Ho_Chi_Minh)")
    print(f"  Target Slots: {slots}")
    print(f"  First Slot:   {slots_dt[0].isoformat()}")
    print(f"  Session ID:   {session_id}")
    print(f"  Dry Run:      {args.dry_run}")

    # 3. Path resolution for shared state ledger
    # Resolve canonical state path from base config if not explicitly provided
    if args.state_file:
        state_path = args.state_file.resolve()
    else:
        state_path = base_cfg.output("state/capture.sqlite").resolve()

    if not state_path.is_file():
        raise FileNotFoundError(
            f"State ledger not found at {state_path}!\n"
            "State must be restored and verified before running trial."
        )

    # 4. Construct trial configuration and output directories
    trial_root = base_cfg.study.project_root / f".local/pilots/runner_trial_3slots/{session_id}"
    trial_root.mkdir(parents=True, exist_ok=True)
    trial_plan_file = trial_root / "trial_plan.json"
    trial_config_file = trial_root / "trial_study.yaml"
    trial_metadata_file = trial_root / "trial_metadata.json"

    trial_study = base_cfg.study.model_copy(update={
        "study_id": f"hcm_urban_trial_{session_id}",
        "schedule_version": f"runner_trial_5m_{slots[0].replace(':', '')}_{slots[-1].replace(':', '')}_v1",
        "slots": slots,
        "dates": [target_date],
        "pilot_run_root": trial_root,
        "capture_plan_file": trial_plan_file,
    })
    trial_cfg = ResolvedConfig(trial_study, base_cfg.policy, base_cfg.sources, base_cfg.budget)

    print("\nSTEP 3: Building Trial Capture Plan & Config...")
    plan = build_capture_plan(paths, graph, trial_cfg)
    write_json(trial_plan_file, plan.model_dump(mode="json"), immutable=False)
    print(f"  Trial plan written to: {trial_plan_file}")

    with open(trial_config_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(trial_study.model_dump(mode="json"), f, sort_keys=False)
    print(f"  Trial config written to: {trial_config_file}")

    meta = {
        "session_id": session_id,
        "trial_root": str(trial_root.resolve()),
        "target_date": date_str,
        "slots": slots,
        "dry_run": args.dry_run,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_json(trial_metadata_file, meta, immutable=False)

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as gh_out:
            gh_out.write(f"session_id={session_id}\n")
            gh_out.write(f"trial_root={trial_root.resolve()}\n")
            gh_out.write(f"actual_date={date_str}\n")

    # 5. Preflight check with shared state ledger
    state = State(state_path)
    clock = Clock()
    print("\nSTEP 4: Running Preflight Checks with Shared Ledger...")
    check = preflight(
        plan, trial_cfg,
        allow_online=args.allow_online,
        state=state,
        day=target_date,
        clock=clock,
    )

    traffic_day = check["providers"]["traffic"]["forecast_by_accounting_day"].get(date_str, {})
    weather_day = check["providers"]["weather"]["forecast_by_accounting_day"].get(date_str, {})

    print(f"  Ready:   {check['ready']}")
    print(f"  Reasons: {check['reasons']}")
    print(f"  Traffic Budget ({date_str}): used={traffic_day.get('used')}, remaining={traffic_day.get('remaining')}, required={traffic_day.get('required_attempts_worst_case')}")
    print(f"  Weather Budget ({date_str}): used={weather_day.get('used')}, remaining={weather_day.get('remaining')}, required={weather_day.get('required_attempts_worst_case')}")

    if not check["ready"]:
        print(f"\nERROR: Preflight failed with reasons: {check['reasons']}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("\nSUCCESS: Preflight dry-run completed cleanly. (No live API calls made).")
        return 0

    # 6. Timing verification: Ensure setup did not eat safety margin
    now_before_capture = datetime.now(tz)
    seconds_to_slot_1 = (slots_dt[0] - now_before_capture).total_seconds()
    print(f"\nTime remaining until Slot 1 ({slots[0]}): {seconds_to_slot_1:.1f}s")
    if seconds_to_slot_1 < 60:
        raise RuntimeError(
            f"PREFLIGHT_TIMING_VIOLATION: Setup consumed too much time. "
            f"Only {seconds_to_slot_1:.1f}s remain until slot 1 (< 60s minimum margin). "
            f"Aborting execution to prevent false-success missed slots."
        )

    # 7. Execute live capture and ETL
    print("\nSTEP 5: Executing Live Capture and Offline Processing Pipeline...")
    result = run_day(
        trial_cfg, target_date,
        allow_online=args.allow_online,
        state=state,
        clock=clock,
    )

    print("\nSTEP 6: Evaluating Acceptance Criteria...")
    print(f"  Status:             {result.get('status')}")
    print(f"  Slots executed:     {result.get('slots')}")
    print(f"  Raw status counts:  {result.get('raw_status_counts')}")
    print(f"  Errors:             {result.get('errors')}")

    raw_counts = result.get("raw_status_counts", {})
    if (
        result.get("status") != "complete"
        or raw_counts.get("complete") != 3
        or raw_counts.get("missed", 0) != 0
        or raw_counts.get("partial", 0) != 0
        or raw_counts.get("interrupted", 0) != 0
        or len(result.get("errors", [])) != 0
    ):
        print("\nERROR: Trial run failed to meet strict completion invariants!", file=sys.stderr)
        return 1

    # 8. Check publication observations table
    pub_csv = trial_root / "interim" / "publication" / "observations_full.csv"
    audit_res = verify_publication_results(pub_csv, date_str, slots, paths)
    print("\nSTEP 7: Publication Data Quality Verification:")
    print(f"  Total Rows:             {audit_res['total_rows']} (expected {audit_res['expected_keys_count']})")
    print(f"  Unique Keys:            {audit_res['unique_keys']} / {audit_res['expected_keys_count']}")
    print(f"  Speed Complete:         {audit_res['speed_complete']} / 39")
    print(f"  CI Complete:            {audit_res['ci_complete']} / 39")
    print(f"  Weather Complete:       {audit_res['weather_complete']} / 39")
    print(f"  All Metrics Complete:   {audit_res['all_metrics_complete']} / 39")

    if (
        audit_res["total_rows"] != 39
        or audit_res["has_duplicates"]
        or audit_res["missing_keys"]
        or audit_res["unexpected_keys"]
    ):
        print("\nERROR: Observations table validation failed!", file=sys.stderr)
        if audit_res["missing_keys"]:
            print(f"  Missing Keys ({len(audit_res['missing_keys'])}): {audit_res['missing_keys']}", file=sys.stderr)
        if audit_res["unexpected_keys"]:
            print(f"  Unexpected Keys ({len(audit_res['unexpected_keys'])}): {audit_res['unexpected_keys']}", file=sys.stderr)
        return 1

    print("\n" + "=" * 80)
    print("TRIAL RUN COMPLETE & ACCEPTED: 3/3 snapshots complete, 39 rows verified.")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())
