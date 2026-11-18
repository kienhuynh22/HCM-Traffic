"""
State Ledger & Checkpoint Manager for HCM-Traffic.
Handles:
1. Transactional import of external session usage with stable source IDs (idempotent, auditable).
2. Consistent WAL-flushed SQLite checkpoints with integrity verification and provenance manifests.
3. Safe restore with strict missing-state rejection (allow_init_state defaults to False).
4. Detailed daily and cumulative budget summaries for requested dates.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, date, time, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tarfile
import tempfile
from zoneinfo import ZoneInfo

from src.storage.state import State


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def ensure_reconciliation_table(db: sqlite3.Connection):
    """Ensure audit table for tracked external usage exists without modifying core schema."""
    db.execute("""
    CREATE TABLE IF NOT EXISTS imported_sources(
        source_id TEXT NOT NULL,
        accounting_date TEXT NOT NULL,
        provider TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        imported_at TEXT NOT NULL,
        PRIMARY KEY(source_id, accounting_date, provider)
    );
    """)


def ensure_provenance_table(db: sqlite3.Connection):
    """Ensure provenance metadata table exists to track study_id and registry_version."""
    db.execute("""
    CREATE TABLE IF NOT EXISTS state_provenance(
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)


def flush_and_verify_sqlite(db_path: Path) -> dict:
    """Flush WAL and execute PRAGMA integrity_check."""
    if not db_path.is_file():
        raise FileNotFoundError(f"Database file does not exist: {db_path}")

    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        # Checkpoint WAL to flush all transactions into main .sqlite file
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        cursor = conn.execute("PRAGMA integrity_check;")
        rows = [r[0] for r in cursor.fetchall()]
        if rows != ["ok"]:
            raise ValueError(f"SQLite integrity check failed on {db_path}: {rows}")

        # Gather quick counts
        cursor = conn.execute("SELECT provider, COUNT(*) FROM attempts GROUP BY provider;")
        provider_counts = dict(cursor.fetchall())
        return {"status": "ok", "provider_counts": provider_counts}
    finally:
        conn.close()


def get_daily_provider_counts(db_path: Path, accounting_tz: str = "Asia/Ho_Chi_Minh") -> dict[tuple[str, str], int]:
    """
    Returns a dict mapping (accounting_date_str, provider) -> count of attempts in db_path.
    accounting_date_str is YYYY-MM-DD in accounting_tz.
    """
    if not db_path.is_file():
        return {}
    zone = ZoneInfo(accounting_tz)
    counts: dict[tuple[str, str], int] = {}
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='attempts'")
        if not cur.fetchone():
            return counts
        cur.execute("SELECT provider, reserved_at FROM attempts")
        for prov, res_at in cur.fetchall():
            if not res_at:
                continue
            try:
                clean_ts = str(res_at).replace("Z", "+00:00")
                if " " in clean_ts and "T" not in clean_ts:
                    clean_ts = clean_ts.replace(" ", "T")
                dt = datetime.fromisoformat(clean_ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                local_d = dt.astimezone(zone).date().isoformat()
                k = (local_d, str(prov))
                counts[k] = counts.get(k, 0) + 1
            except Exception:
                continue
    finally:
        conn.close()
    return counts


def create_consistent_backup(src_db: Path, dest_db: Path):
    """Use sqlite3 online backup API to copy database cleanly without WAL/SHM locks."""
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(str(src_db), timeout=30)
    dest_conn = sqlite3.connect(str(dest_db), timeout=30)
    try:
        src_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        src_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        src_conn.close()


def import_external_usage(
    state_file: Path,
    source_id: str,
    target_date: date,
    traffic_count: int,
    weather_count: int,
    timezone_name: str = "Asia/Ho_Chi_Minh",
    allow_init: bool = False,
) -> dict:
    """
    Import external usage into state ledger idempotently.
    Uses 'external_reconciled' outcome and tracks source_id in imported_sources.
    """
    if traffic_count < 0 or weather_count < 0:
        raise ValueError(f"Attempt counts cannot be negative: traffic={traffic_count}, weather={weather_count}")
    if not source_id or not source_id.strip():
        raise ValueError("source_id must be a non-empty stable string identifier")

    if not state_file.is_file():
        if not allow_init:
            raise FileNotFoundError(
                f"State file {state_file} does not exist. Cannot import usage without an initialized state. "
                "Pass allow_init=True if deliberately bootstrapping a new ledger."
            )
        state_file.parent.mkdir(parents=True, exist_ok=True)
        State(state_file)

    tz = ZoneInfo(timezone_name)
    # Midday timestamp in target date
    midday = datetime.combine(target_date, time(hour=12, minute=0), tz).astimezone(timezone.utc)
    midday_iso = midday.isoformat()
    date_str = target_date.isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(str(state_file), timeout=30)
    conn.isolation_level = None  # Explicit transaction control
    try:
        conn.execute("BEGIN IMMEDIATE;")
        ensure_reconciliation_table(conn)

        results = {}
        for provider, count in [("traffic", traffic_count), ("weather", weather_count)]:
            row = conn.execute(
                "SELECT attempt_count FROM imported_sources WHERE source_id=? AND accounting_date=? AND provider=?",
                (source_id, date_str, provider),
            ).fetchone()

            if row is not None:
                existing_count = row[0]
                if existing_count == count:
                    results[provider] = {"status": "already_imported", "count": count}
                    continue
                else:
                    raise ValueError(
                        f"Conflict for source '{source_id}' on {date_str} for {provider}: "
                        f"previously imported {existing_count}, now requested {count}."
                    )

            # Insert attempts with outcome='external_reconciled'
            # Using unique resource naming bound to source_id to prevent collision
            snapshot_key = f"ext_{source_id}_{date_str}"
            scope_key = f"external:{source_id}"
            for i in range(1, count + 1):
                resource_key = f"ext_{source_id}_{provider}_{i}"
                conn.execute(
                    """
                    INSERT INTO attempts(scope, provider, snapshot, resource, number, reserved_at, outcome)
                    VALUES(?, ?, ?, ?, 1, ?, 'external_reconciled');
                    """,
                    (scope_key, provider, snapshot_key, resource_key, midday_iso),
                )

            conn.execute(
                """
                INSERT INTO imported_sources(source_id, accounting_date, provider, attempt_count, imported_at)
                VALUES(?, ?, ?, ?, ?);
                """,
                (source_id, date_str, provider, count, now_iso),
            )
            results[provider] = {"status": "imported", "count": count}

        conn.execute("COMMIT;")
        return results
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()


def create_checkpoint(
    state_file: Path,
    checkpoint_dir: Path,
    metadata: dict,
) -> Path:
    """Create a consistent, WAL-flushed SQLite checkpoint tarball with manifest."""
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    if not state_file.is_file():
        raise FileNotFoundError(f"Cannot create checkpoint: state file {state_file} does not exist.")

    # 1. Flush & verify SQLite
    integrity = flush_and_verify_sqlite(state_file)
    if integrity["status"] != "ok":
        raise ValueError("Cannot checkpoint corrupt SQLite database")

    # 2. Consistent copy to staging
    staging_db = checkpoint_dir / "capture.sqlite"
    create_consistent_backup(state_file, staging_db)

    # Record provenance into staging DB
    conn = sqlite3.connect(str(staging_db), timeout=30)
    try:
        ensure_provenance_table(conn)
        for k, v in metadata.items():
            if v:
                conn.execute("INSERT OR REPLACE INTO state_provenance(key, value) VALUES(?, ?);", (str(k), str(v)))
        conn.commit()
    finally:
        conn.close()

    # 3. Checksums and manifest
    sqlite_sha = sha256_file(staging_db)
    state = State(staging_db)
    manifest = {
        "checkpoint_version": "v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sqlite_sha256": sqlite_sha,
        "sqlite_size_bytes": staging_db.stat().st_size,
        "integrity_check": "ok",
        "cumulative_counts": state.counts(),
        **metadata,
    }

    manifest_file = checkpoint_dir / "checkpoint_manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # 4. Pack into checkpoint archive
    archive_path = checkpoint_dir / "state_checkpoint.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(staging_db, arcname="capture.sqlite")
        tar.add(manifest_file, arcname="checkpoint_manifest.json")

    archive_sha = sha256_file(archive_path)
    (checkpoint_dir / "state_checkpoint.tar.gz.sha256").write_text(
        f"{archive_sha}  {archive_path.name}\n", encoding="utf-8"
    )

    # Clean up uncompressed staging files
    staging_db.unlink()
    manifest_file.unlink()

    return archive_path


def restore_checkpoint(
    checkpoint_archive: Path,
    target_state_file: Path,
    expected_scope: dict,
    allow_init: bool = False,
) -> dict:
    """
    Restore state database from checkpoint archive after rigorous verification.
    If checkpoint is missing:
      - if allow_init is False: raises RuntimeError (stops silent reset).
      - if allow_init is True: creates fresh state.
    """
    if not checkpoint_archive.is_file():
        if not allow_init:
            raise RuntimeError(
                f"FATAL: State checkpoint archive not found at '{checkpoint_archive}'. "
                "Runner will NOT silently reset daily usage ledger to 0. "
                "If this is a deliberate fresh run, pass allow_init_state=True."
            )
        print(f"No checkpoint found. Initializing fresh state ledger at {target_state_file} (allow_init=True)...")
        target_state_file.parent.mkdir(parents=True, exist_ok=True)
        State(target_state_file)
        return {"status": "initialized_fresh", "file": str(target_state_file)}

    print(f"Extracting and verifying checkpoint from {checkpoint_archive}...")
    staging_dir = checkpoint_archive.parent / f"_staging_restore_{os.getpid()}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(checkpoint_archive, "r:gz") as tar:
            # Check members for path traversal
            for m in tar.getmembers():
                if ".." in m.name or m.name.startswith(("/", "\\")):
                    raise SecurityError(f"Illegal path in checkpoint member: {m.name}")
            tar.extractall(path=staging_dir)

        manifest_file = staging_dir / "checkpoint_manifest.json"
        restored_db = staging_dir / "capture.sqlite"
        if not manifest_file.is_file() or not restored_db.is_file():
            raise FileNotFoundError("Checkpoint archive missing manifest or SQLite database.")

        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        actual_sha = sha256_file(restored_db)
        if actual_sha != manifest.get("sqlite_sha256"):
            raise ValueError(
                f"Checkpoint SHA-256 mismatch! Manifest: {manifest.get('sqlite_sha256')}, Actual: {actual_sha}"
            )

        # Verify scope compatibility
        for key in ("study_id", "registry_version"):
            if key in expected_scope and manifest.get(key) != expected_scope[key]:
                raise ValueError(
                    f"Checkpoint scope mismatch on {key}: expected '{expected_scope[key]}', "
                    f"checkpoint has '{manifest.get(key)}'."
                )

        # Integrity check SQLite
        integrity = flush_and_verify_sqlite(restored_db)
        if integrity["status"] != "ok":
            raise ValueError("Restored database failed SQLite integrity check")

        # Record and verify provenance in restored DB
        conn = sqlite3.connect(str(restored_db), timeout=30)
        try:
            ensure_provenance_table(conn)
            cur = conn.cursor()
            for key in ("study_id", "registry_version"):
                if key in expected_scope and expected_scope[key]:
                    cur.execute("SELECT value FROM state_provenance WHERE key=?", (key,))
                    row = cur.fetchone()
                    if row and row[0] != expected_scope[key]:
                        raise ValueError(
                            f"Restored database provenance mismatch on {key}: "
                            f"db has '{row[0]}', expected '{expected_scope[key]}'"
                        )
                    elif not row:
                        cur.execute("INSERT OR REPLACE INTO state_provenance(key, value) VALUES(?, ?)", (key, expected_scope[key]))
            conn.commit()
        finally:
            conn.close()

        # Atomic installation to target path
        target_state_file.parent.mkdir(parents=True, exist_ok=True)
        create_consistent_backup(restored_db, target_state_file)
        print(f"Checkpoint successfully restored to {target_state_file}.")
        return {"status": "restored", "manifest": manifest, "file": str(target_state_file)}
    finally:
        # Clean staging
        if staging_dir.exists():
            for f in staging_dir.iterdir():
                try:
                    f.unlink()
                except OSError:
                    pass
            try:
                staging_dir.rmdir()
            except OSError:
                pass


def inspect_and_select_latest_checkpoint(
    candidates: list,
    expected_scope: dict,
) -> tuple[Path, dict]:
    """
    Given candidate checkpoint archives (or candidate dicts containing run_id, created_at, archive_path),
    verifies integrity and scope, selecting the newest checkpoint.
    SAFETY INVARIANT:
    Evaluates candidates in order (newest first). If the newest candidate run has a checkpoint
    that fails download or verification, it raises RuntimeError immediately.
    It refuses silent fallback to older checkpoints to prevent quota regression.
    """
    if not candidates:
        raise ValueError("No checkpoint candidates provided.")

    normalized = []
    for item in candidates:
        if isinstance(item, dict):
            normalized.append({
                "run_id": str(item.get("run_id", "unknown")),
                "created_at": str(item.get("created_at", "")),
                "archive_path": Path(item["archive_path"]),
                "download_error": str(item.get("download_error", "")),
            })
        else:
            p = Path(item)
            normalized.append({
                "run_id": p.name,
                "created_at": "",
                "archive_path": p,
                "download_error": "",
            })

    # Sort candidates newest first if created_at is present
    if any(n["created_at"] for n in normalized):
        normalized.sort(key=lambda x: x["created_at"], reverse=True)

    newest_run_id = normalized[0]["run_id"]
    errors_by_run = {}
    valid_candidates = []

    for item in normalized:
        archive_path = item["archive_path"]
        run_id = item["run_id"]
        dl_err = item.get("download_error")

        if dl_err:
            err_msg = f"Download failed: {dl_err}"
            errors_by_run[run_id] = err_msg
            if run_id == newest_run_id:
                raise RuntimeError(
                    f"FATAL: Checkpoint download from newest run '{run_id}' failed: {dl_err}. "
                    "Refusing fallback to older runs to prevent ledger regression."
                )
            continue

        if not archive_path.is_file():
            err_msg = f"Archive file missing: {archive_path}"
            errors_by_run[run_id] = err_msg
            if run_id == newest_run_id:
                raise RuntimeError(
                    f"FATAL: Checkpoint from newest run '{run_id}' failed: {err_msg}. "
                    "Refusing fallback to older runs to prevent ledger regression."
                )
            continue

        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                for m in tar.getmembers():
                    if ".." in m.name or m.name.startswith(("/", "\\")):
                        raise SecurityError(f"Illegal path in checkpoint member: {m.name}")
                manifest_member = tar.extractfile("checkpoint_manifest.json")
                if not manifest_member:
                    raise FileNotFoundError("checkpoint_manifest.json not found in archive")
                manifest = json.loads(manifest_member.read().decode("utf-8"))

            for k in ("study_id", "registry_version"):
                if k in expected_scope and expected_scope[k]:
                    if manifest.get(k) != expected_scope[k]:
                        raise ValueError(f"Scope mismatch on {k}: expected '{expected_scope[k]}', got '{manifest.get(k)}'")

            created_at = manifest.get("created_at_utc", item["created_at"])
            counts = manifest.get("cumulative_counts", {})
            total_attempts = sum(counts.values()) if isinstance(counts, dict) else 0

            valid_candidates.append({
                "run_id": run_id,
                "archive_path": archive_path,
                "manifest": manifest,
                "created_at": created_at,
                "total_attempts": total_attempts,
            })
        except Exception as exc:
            err_msg = str(exc)
            errors_by_run[run_id] = err_msg
            if run_id == newest_run_id:
                raise RuntimeError(
                    f"FATAL: Checkpoint from newest run '{run_id}' failed verification: {err_msg}. "
                    "Refusing fallback to older runs to prevent ledger regression."
                )
            continue

    if not valid_candidates:
        raise ValueError(f"No valid checkpoints matching expected scope found. Errors: {errors_by_run}")

    # Sort valid candidates by created_at descending, then total_attempts descending
    valid_candidates.sort(key=lambda c: (c["created_at"], c["total_attempts"]), reverse=True)
    best = valid_candidates[0]
    return best["archive_path"], best["manifest"]


def main():
    parser = argparse.ArgumentParser(description="HCM-Traffic State Ledger & Checkpoint Manager.")
    subparsers = parser.add_subparsers(dest="action", required=True)

    # 1. verify-restore
    vr = subparsers.add_parser("verify-restore")
    vr.add_argument("--state-file", type=Path, required=True)
    vr.add_argument("--checkpoint-archive", type=Path, default=None)
    vr.add_argument("--allow-init", action="store_true", default=False)
    vr.add_argument("--study-id", type=str, default="")
    vr.add_argument("--registry-version", type=str, default="")

    # 2. create-checkpoint
    cc = subparsers.add_parser("create-checkpoint")
    cc.add_argument("--state-file", type=Path, required=True)
    cc.add_argument("--checkpoint-dir", type=Path, required=True)
    cc.add_argument("--study-id", type=str, default="")
    cc.add_argument("--registry-version", type=str, default="")
    cc.add_argument("--run-id", type=str, default="")
    cc.add_argument("--run-attempt", type=str, default="1")
    cc.add_argument("--branch", type=str, default="")
    cc.add_argument("--workflow", type=str, default="capture_13paths")

    # 3. import-usage
    iu = subparsers.add_parser("import-usage")
    iu.add_argument("--state-file", type=Path, required=True)
    iu.add_argument("--source-id", type=str, required=True, help="Stable identifier of external session (e.g. local_pilot_20261118_v1)")
    iu.add_argument("--date", type=str, required=True, help="Accounting date YYYY-MM-DD")
    iu.add_argument("--traffic", type=int, required=True, help="Exact number of external traffic attempts to import")
    iu.add_argument("--weather", type=int, required=True, help="Exact number of external weather attempts to import")
    iu.add_argument("--timezone", type=str, default="Asia/Ho_Chi_Minh")
    iu.add_argument("--allow-init", action="store_true", default=False)

    # 4. summary
    sm = subparsers.add_parser("summary")
    sm.add_argument("--state-file", type=Path, required=True)
    sm.add_argument("--date", type=str, default="", help="Specific accounting date to report (defaults to today)")
    sm.add_argument("--timezone", type=str, default="Asia/Ho_Chi_Minh")

    args = parser.parse_args()

    if args.action == "verify-restore":
        expected = {}
        if args.study_id:
            expected["study_id"] = args.study_id
        if args.registry_version:
            expected["registry_version"] = args.registry_version

        state_path = args.state_file.resolve()
        checkpoint = args.checkpoint_archive.resolve() if args.checkpoint_archive else None

        if state_path.is_file():
            # State already present (e.g. from runner cache)
            print(f"Existing state file found at {state_path}. Verifying integrity and provenance...")
            integrity = flush_and_verify_sqlite(state_path)
            if integrity.get("status") != "ok":
                raise ValueError(f"SQLite integrity check failed on existing state file: {state_path}")

            # Verify provenance against expected scope
            conn = sqlite3.connect(str(state_path), timeout=30)
            try:
                ensure_provenance_table(conn)
                cur = conn.cursor()
                for key in ("study_id", "registry_version"):
                    if key in expected and expected[key]:
                        cur.execute("SELECT value FROM state_provenance WHERE key=?", (key,))
                        row = cur.fetchone()
                        if row and row[0] != expected[key]:
                            raise ValueError(
                                f"Existing state file provenance mismatch on {key}: "
                                f"ledger has '{row[0]}', expected '{expected[key]}'"
                            )
                        elif not row:
                            cur.execute("INSERT OR REPLACE INTO state_provenance(key, value) VALUES(?, ?)", (key, expected[key]))
                conn.commit()
            finally:
                conn.close()

            # If checkpoint archive is also present, inspect against existing state
            if checkpoint and checkpoint.is_file():
                print(f"Inspecting accompanying checkpoint archive {checkpoint} against existing state...")
                with tarfile.open(checkpoint, "r:gz") as tar:
                    m = tar.extractfile("checkpoint_manifest.json")
                    chk_man = json.loads(m.read().decode("utf-8")) if m else {}

                for key in ("study_id", "registry_version"):
                    if key in expected and chk_man.get(key) != expected[key]:
                        raise ValueError(
                            f"Accompanying checkpoint scope mismatch on {key}: "
                            f"checkpoint has '{chk_man.get(key)}', expected '{expected[key]}'"
                        )

                chk_counts = chk_man.get("cumulative_counts", {})
                chk_total = sum(chk_counts.values()) if isinstance(chk_counts, dict) else 0
                state_counts = State(state_path).counts()
                state_total = sum(state_counts.values())
                chk_created_at = chk_man.get("created_at_utc", "")

                # Query existing created_at_utc from state_provenance
                state_created_at = ""
                conn_prov = sqlite3.connect(str(state_path), timeout=30)
                try:
                    cur_p = conn_prov.cursor()
                    cur_p.execute("SELECT value FROM state_provenance WHERE key='created_at_utc'")
                    row_p = cur_p.fetchone()
                    if row_p:
                        state_created_at = row_p[0]
                finally:
                    conn_prov.close()

                # Extract checkpoint's SQLite to temporary staging to compare detailed accounting date counts
                with tempfile.TemporaryDirectory(prefix="_chk_inspect_") as tmp_insp:
                    tmp_insp_dir = Path(tmp_insp)
                    with tarfile.open(checkpoint, "r:gz") as tar:
                        tar.extract("capture.sqlite", path=tmp_insp_dir)
                    chk_sqlite_path = tmp_insp_dir / "capture.sqlite"
                    chk_daily = get_daily_provider_counts(chk_sqlite_path)
                    state_daily = get_daily_provider_counts(state_path)

                # Check 1: Monotonicity across cumulative provider counts
                all_providers = set(state_counts.keys()) | set(chk_counts.keys()) | {"traffic", "weather"}
                regressions = []
                for p in all_providers:
                    s_cnt = state_counts.get(p, 0)
                    c_cnt = chk_counts.get(p, 0)
                    if c_cnt < s_cnt:
                        regressions.append(f"provider '{p}': checkpoint={c_cnt} < state={s_cnt}")

                # Check 2: Monotonicity across accounting dates
                all_date_prov_keys = set(state_daily.keys()) | set(chk_daily.keys())
                for d_key, p_name in all_date_prov_keys:
                    s_cnt = state_daily.get((d_key, p_name), 0)
                    c_cnt = chk_daily.get((d_key, p_name), 0)
                    if c_cnt < s_cnt:
                        regressions.append(f"date {d_key} provider '{p_name}': checkpoint={c_cnt} < state={s_cnt}")

                if regressions:
                    raise ValueError(
                        f"FATAL: Checkpoint ({chk_man.get('run_id', 'unknown')}) contradicts existing state! "
                        f"Checkpoint would decrease attempts: {', '.join(regressions)}. "
                        f"A newer timestamp ({chk_created_at} vs {state_created_at}) is NOT permitted to overwrite "
                        f"a ledger with lower attempts. Halting job and preserving existing SQLite."
                    )

                # If checkpoint is valid and has higher usage than cached DB (or identical usage with newer timestamp),
                # checkpoint takes precedence
                if chk_total > state_total or (chk_total == state_total and chk_created_at and chk_created_at > state_created_at):
                    print(
                        f"Checkpoint ({chk_man.get('run_id', 'unknown')}, total={chk_total}, created={chk_created_at}) "
                        f"supersedes existing state (total={state_total}, created={state_created_at}). "
                        f"Updating state file with latest verified checkpoint..."
                    )
                    res = restore_checkpoint(checkpoint, state_path, expected, allow_init=False)
                    state = State(state_path)
                    print(f"State file successfully updated to latest checkpoint. Cumulative counts: {state.counts()}")
                    return 0

            state = State(state_path)
            print(f"Integrity and provenance check passed. Cumulative counts: {state.counts()}")
            return 0
        elif checkpoint and checkpoint.is_file():
            res = restore_checkpoint(checkpoint, state_path, expected, allow_init=args.allow_init)
            state = State(state_path)
            print(f"Restore result: {res['status']}. Cumulative counts: {state.counts()}")
            return 0
        else:
            if not args.allow_init:
                print(
                    f"ERROR: Neither state file ({state_path}) nor valid checkpoint ({checkpoint}) was found!\n"
                    "Silently resetting daily budget to 0 is prohibited.\n"
                    "If this is a deliberate fresh bootstrap, pass --allow-init.",
                    file=sys.stderr,
                )
                return 1
            print(f"Initializing fresh state ledger at {state_path} (allow_init=True)...")
            state_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(state_path))
            ensure_provenance_table(conn)
            for k, v in expected.items():
                if v:
                    conn.execute("INSERT OR REPLACE INTO state_provenance(key, value) VALUES(?, ?);", (k, v))
            conn.commit()
            conn.close()
            state = State(state_path)
            print(f"Fresh state ledger initialized at {state_path}.")
            return 0

    elif args.action == "create-checkpoint":
        state_path = args.state_file.resolve()
        metadata = {
            "study_id": args.study_id,
            "registry_version": args.registry_version,
            "run_id": args.run_id,
            "run_attempt": args.run_attempt,
            "branch": args.branch,
            "workflow": args.workflow,
        }
        archive = create_checkpoint(state_path, args.checkpoint_dir.resolve(), metadata)
        print(f"Checkpoint successfully created: {archive}")
        return 0

    elif args.action == "import-usage":
        target_date = date.fromisoformat(args.date)
        state_path = args.state_file.resolve()
        res = import_external_usage(
            state_path,
            args.source_id,
            target_date,
            args.traffic,
            args.weather,
            args.timezone,
            allow_init=args.allow_init,
        )
        print(f"Import usage result for {args.source_id} on {args.date}:")
        for p, r in res.items():
            print(f"  {p}: {r['status']} ({r['count']} attempts)")

        state = State(state_path)
        tz = ZoneInfo(args.timezone)
        dt_target = datetime.combine(target_date, time(12, 0), tz).astimezone(timezone.utc)
        print(f"Updated daily counts for {args.date} ({args.timezone}): {state.daily_counts(dt_target, args.timezone)}")
        return 0

    elif args.action == "summary":
        state_path = args.state_file.resolve()
        if not state_path.is_file():
            print(f"ERROR: State file {state_path} does not exist.", file=sys.stderr)
            return 1

        integrity = flush_and_verify_sqlite(state_path)
        state = State(state_path)
        tz = ZoneInfo(args.timezone)

        if args.date:
            target_date = date.fromisoformat(args.date)
            dt_target = datetime.combine(target_date, time(12, 0), tz).astimezone(timezone.utc)
        else:
            dt_target = datetime.now(timezone.utc)
            target_date = dt_target.astimezone(tz).date()

        daily = state.daily_counts(dt_target, args.timezone)
        total = state.counts()

        conn = sqlite3.connect(str(state_path))
        ensure_reconciliation_table(conn)
        cursor = conn.execute("SELECT source_id, accounting_date, provider, attempt_count, imported_at FROM imported_sources ORDER BY imported_at;")
        imported = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]
        conn.close()

        print("=" * 60)
        print("STATE LEDGER SUMMARY")
        print(f"Database:        {state_path}")
        print(f"Integrity check: {integrity['status']}")
        print(f"Cumulative:      {total}")
        print(f"Date ({args.timezone}): {target_date} -> {daily}")
        if imported:
            print("\nImported External Usage Sources:")
            for item in imported:
                print(f"  - [{item['accounting_date']}] {item['source_id']}: {item['provider']}={item['attempt_count']} (at {item['imported_at']})")
        print("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
