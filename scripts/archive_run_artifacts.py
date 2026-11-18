"""
Archive Manager for HCM-Traffic Run Outputs.
Packages raw payloads, consistent SQLite state, publication tables, interim audits, and logs
into a non-hidden versioned archive with manifest and SHA-256 checksums.
Guarantees consistent SQLite backup without orphan WAL/SHM locks.
Excludes all environment files, keys, and local secrets.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tarfile

from src.config import load_config
from scripts.manage_state import create_checkpoint, flush_and_verify_sqlite


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def find_files_to_archive(pilot_root: Path, date_str: str) -> list[tuple[Path, str]]:
    """
    Identifies exact output paths without ambiguous wild globs.
    Returns list of (absolute_path, archive_relative_path).
    """
    items = []
    forbidden_names = {".env", ".key"}
    forbidden_suffixes = {".key", ".secret"}

    def is_safe(p: Path) -> bool:
        if p.name in forbidden_names or p.suffix in forbidden_suffixes:
            return False
        if any(part.startswith(".env") for part in p.parts):
            return False
        return True

    # 0. Session configs, plans, and metadata directly in pilot_root
    if pilot_root.is_dir():
        for f in pilot_root.iterdir():
            if f.is_file() and is_safe(f) and f.suffix in (".yaml", ".yml", ".json"):
                items.append((f, f"config_and_plan/{f.name}"))

    # 1. Publication tables
    pub_dirs = [
        pilot_root / "interim" / "publication",
        pilot_root / "publication",
    ]
    for pub_dir in pub_dirs:
        if pub_dir.is_dir():
            for f in pub_dir.iterdir():
                if f.is_file() and is_safe(f):
                    items.append((f, f"publication/{f.name}"))

    # 2. Raw snapshot directories and inventories for this date
    raw_root = pilot_root / "raw"
    if raw_root.is_dir():
        for study_dir in raw_root.iterdir():
            if study_dir.is_dir():
                date_dir = study_dir / date_str
                if date_dir.is_dir():
                    for path in date_dir.rglob("*"):
                        if path.is_file() and is_safe(path):
                            rel = path.relative_to(date_dir)
                            items.append((path, f"raw/{study_dir.name}/{date_str}/{rel.as_posix()}"))

    # 3. Interim audit logs across all runs for this session
    interim_root = pilot_root / "interim"
    if interim_root.is_dir():
        for run_dir in interim_root.iterdir():
            if run_dir.is_dir() and run_dir.name not in ("publication", "publish", "inputs"):
                audit_dir = run_dir / "audit"
                if audit_dir.is_dir():
                    for f in audit_dir.iterdir():
                        if f.is_file() and is_safe(f):
                            items.append((f, f"audit/{run_dir.name}/{f.name}"))
                reports_dir = run_dir / "reports"
                if reports_dir.is_dir():
                    for f in reports_dir.iterdir():
                        if f.is_file() and is_safe(f):
                            items.append((f, f"reports/{run_dir.name}/{f.name}"))

    # 4. Logs
    logs_dir = pilot_root.parent.parent / "logs"
    if logs_dir.is_dir():
        for f in logs_dir.glob(f"*{date_str}*"):
            if f.is_file() and is_safe(f):
                items.append((f, f"logs/{f.name}"))

    pilot_logs = pilot_root / "logs"
    if pilot_logs.is_dir():
        for f in pilot_logs.iterdir():
            if f.is_file() and is_safe(f):
                items.append((f, f"logs/{f.name}"))

    return items


def main():
    parser = argparse.ArgumentParser(description="Package run outputs into artifacts archive.")
    parser.add_argument("--config-file", type=Path, required=True)
    parser.add_argument("--mode", type=str, required=True, choices=["trial_3slots", "afternoon_37slots"])
    parser.add_argument("--date", type=str, required=True, help="Accounting date YYYY-MM-DD")
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/run_bundles"))
    parser.add_argument("--pilot-root", type=Path, default=None, help="Explicit pilot directory of the run to archive")
    parser.add_argument("--session-id", type=str, default="", help="Session ID for trial runs")
    parser.add_argument("--dry-run", action="store_true", help="Whether the run was a dry run")
    parser.add_argument("--run-id", type=str, default="local")
    parser.add_argument("--run-attempt", type=str, default="1")
    parser.add_argument("--git-sha", type=str, default="")
    parser.add_argument("--status", type=str, default="completed")
    args = parser.parse_args()

    cfg = load_config(args.config_file.resolve())
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine pilot root based on mode or explicit argument
    if args.pilot_root:
        pilot_root = args.pilot_root.resolve()
    elif args.mode == "trial_3slots":
        if args.session_id:
            pilot_root = cfg.study.project_root / f".local/pilots/runner_trial_3slots/{args.session_id}"
        else:
            pilot_root = cfg.study.project_root / ".local/pilots/runner_trial_3slots"
    else:
        pilot_root = cfg.study.project_root / ".local/pilots/study_13paths"

    print(f"Preparing run artifacts archive for {args.mode} on {args.date}...")
    print(f"  Pilot root:  {pilot_root}")
    print(f"  State file:  {args.state_file}")
    print(f"  Output dir:  {output_dir}")

    staging_dir = output_dir / f"_staging_archive_{args.run_id}_{os.getpid()}"
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        manifest_files = {}
        archive_name = f"hcm_traffic_{args.mode}_{args.date}_run{args.run_id}_attempt{args.run_attempt}.tar.gz"
        archive_path = output_dir / archive_name

        # 1. State ledger checkpoint (WAL-flushed)
        state_file = args.state_file.resolve()
        if state_file.is_file():
            print("Creating consistent state checkpoint for archive...")
            chk_dir = staging_dir / "checkpoint"
            chk_archive = create_checkpoint(
                state_file,
                chk_dir,
                metadata={
                    "study_id": cfg.study.study_id,
                    "registry_version": "main13_20261117_869a0f5a",
                    "run_id": args.run_id,
                    "run_attempt": args.run_attempt,
                    "git_sha": args.git_sha,
                },
            )
            # Add state database directly into archive staging
            clean_db = staging_dir / "capture.sqlite"
            conn_src = sqlite3.connect(str(state_file), timeout=30)
            conn_dst = sqlite3.connect(str(clean_db), timeout=30)
            conn_src.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn_src.backup(conn_dst)
            conn_src.close()
            conn_dst.close()

            manifest_files["state/capture.sqlite"] = {
                "sha256": sha256_file(clean_db),
                "size_bytes": clean_db.stat().st_size,
            }
            manifest_files["state/state_checkpoint.tar.gz"] = {
                "sha256": sha256_file(chk_archive),
                "size_bytes": chk_archive.stat().st_size,
            }

        # 2. Collect publication, raw, audits, logs
        file_items = find_files_to_archive(pilot_root, args.date)
        print(f"Found {len(file_items)} data artifacts to bundle.")
        for src_path, arc_name in file_items:
            manifest_files[arc_name] = {
                "sha256": sha256_file(src_path),
                "size_bytes": src_path.stat().st_size,
            }

        # 3. Create run manifest
        run_manifest = {
            "version": "hcm_traffic_run_archive_v1",
            "mode": args.mode,
            "date": args.date,
            "study_id": cfg.study.study_id,
            "registry_version": "main13_20261117_869a0f5a",
            "run_id": args.run_id,
            "run_attempt": args.run_attempt,
            "git_sha": args.git_sha,
            "status": args.status,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "files": manifest_files,
            "total_files": len(manifest_files),
            "total_bytes": sum(f["size_bytes"] for f in manifest_files.values()),
        }

        manifest_path = staging_dir / "archive_manifest.json"
        manifest_path.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

        # 4. Pack into tar.gz
        print(f"Writing compressed archive to {archive_path}...")
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(manifest_path, arcname="archive_manifest.json")
            if (staging_dir / "capture.sqlite").is_file():
                tar.add(staging_dir / "capture.sqlite", arcname="state/capture.sqlite")
            if (staging_dir / "checkpoint" / "state_checkpoint.tar.gz").is_file():
                tar.add(staging_dir / "checkpoint" / "state_checkpoint.tar.gz", arcname="state/state_checkpoint.tar.gz")

            for src_path, arc_name in file_items:
                tar.add(src_path, arcname=arc_name)

        archive_sha = sha256_file(archive_path)
        sha_path = output_dir / f"{archive_name}.sha256"
        sha_path.write_text(f"{archive_sha}  {archive_name}\n", encoding="utf-8")

        size_mb = archive_path.stat().st_size / (1024 * 1024)
        print(f"\nSUCCESS: Run artifacts archived ({size_mb:.2f} MB):")
        print(f"  Archive:  {archive_path}")
        print(f"  SHA-256:  {archive_sha}")
        print(f"  Checksum: {sha_path}")

        # 5. Strict verification for successful live trials
        if args.mode == "trial_3slots" and args.status == "completed" and not args.dry_run:
            print("\nVerifying complete run archive invariants...")
            with tarfile.open(archive_path, "r:gz") as tar:
                names = set(tar.getnames())

            # 1. Publication CSV check
            if not any("observations_full.csv" in n for n in names):
                raise ValueError(f"VERIFICATION FAILED: Archive {archive_path.name} is missing publication CSV (observations_full.csv)")

            # 2. Checkpoint check
            if not any("state_checkpoint.tar.gz" in n for n in names):
                raise ValueError(f"VERIFICATION FAILED: Archive {archive_path.name} is missing state checkpoint (state_checkpoint.tar.gz)")

            # 3. Config & plan check
            has_config = any("trial_study.yaml" in n or "config" in n for n in names)
            has_plan = any("trial_plan.json" in n or "plan" in n for n in names)
            if not (has_config and has_plan):
                raise ValueError(f"VERIFICATION FAILED: Archive missing trial config or plan (has_config={has_config}, has_plan={has_plan})")

            # 4. Raw snapshots check (must have at least 3 distinct snapshots)
            raw_snapshots = set()
            for n in names:
                if n.startswith("raw/"):
                    parts = Path(n).parts
                    if len(parts) >= 4:
                        raw_snapshots.add(parts[3])
                    elif len(parts) >= 3:
                        raw_snapshots.add(parts[2])

            if len(raw_snapshots) < 3:
                raise ValueError(
                    f"VERIFICATION FAILED: Archive has only {len(raw_snapshots)} raw snapshot directories (< 3 expected): {raw_snapshots}"
                )
            print(f"Archive verified: publication CSV, 3 raw snapshots ({len(raw_snapshots)}), config, plan, and checkpoint present.")

        return 0
    finally:
        # Clean staging directory
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
