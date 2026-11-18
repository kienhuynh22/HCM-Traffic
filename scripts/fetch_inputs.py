"""
Secure Fetcher and Verifier for HCM-Traffic Offline Reference Inputs.
Guarantees:
1. Strict archive SHA-256 validation before unpacking.
2. Safe extraction into isolated staging directory with path traversal protection.
3. Verification of every individual file against the embedded package manifest.
4. Atomic installation to target directory only after 100% verification passes.
5. Zero corruption to existing files if download or extraction fails.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

PACKAGE_VERSION = "main13_inputs_v1"
DEFAULT_ASSET_NAME = f"hcm_traffic_inputs_{PACKAGE_VERSION}.tar.gz"
DEFAULT_EXPECTED_SHA = "930bc793558b82e6037208ce12260c5bd3114cd2a235dc4aeecfef6cea0e5006"

REQUIRED_INPUT_FILES = [
    "candidates/main13_20261117_869a0f5a/manifest.json",
    "candidates/main13_20261117_869a0f5a/paths.json",
    "graph/09763400d0887076fc48/manifest.json",
    "graph/09763400d0887076fc48/graph.json",
    "graph/09763400d0887076fc48/graph.sqlite",
    "graph/09763400d0887076fc48/normalized.sqlite",
    "plans/38b28d4596ca3dc3d4ea.json",
    "plans/43da41515fbf56fd910e.json",
    "6fe4fd15df6bb8c5453d/region.osm.pbf",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def check_existing_files(target_dir: Path, manifest_path: Path) -> bool:
    """Check if all files defined in manifest already exist and match their SHA-256."""
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = manifest.get("files")
        if not isinstance(files, dict) or not files:
            return False
        # Check all required files are present
        for req in REQUIRED_INPUT_FILES:
            if req not in files:
                return False
        for rel_path, meta in files.items():
            f_path = target_dir / rel_path
            if not f_path.is_file():
                return False
            if f_path.stat().st_size != meta.get("size_bytes"):
                return False
            if sha256_file(f_path) != meta.get("sha256"):
                return False
        return True
    except Exception:
        return False


def is_safe_member(member: tarfile.TarInfo, staging_dir: Path) -> bool:
    """Security check: Prevent directory traversal, drive letters, and escaping links."""
    # Reject absolute paths or paths with drive letters
    if member.name.startswith(("/", "\\")) or ":" in member.name:
        return False
    # Reject path traversal components
    parts = Path(member.name).parts
    if ".." in parts:
        return False
    # Prevent symlink attacks pointing outside staging
    if member.issym() or member.islnk():
        link_target = Path(member.linkname)
        if link_target.is_absolute() or ".." in link_target.parts:
            return False
    return True


def download_asset(repo: str, tag: str, asset_name: str, dest_dir: Path) -> Path:
    """Download release asset via GitHub CLI or curl with environment token."""
    dest_path = dest_dir / asset_name
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")

    # 1. Try gh CLI
    cmd = [
        "gh", "release", "download", tag,
        "--repo", repo,
        "--pattern", asset_name,
        "--dir", str(dest_dir),
        "--clobber"
    ]
    env = os.environ.copy()
    if token:
        env["GH_TOKEN"] = token

    print(f"Attempting download of '{asset_name}' from {repo}@{tag} via GitHub CLI...")
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode == 0 and dest_path.is_file():
        print("Download via GitHub CLI succeeded.")
        return dest_path

    print(f"GitHub CLI download failed: {result.stderr.strip() or 'Command failed'}. Trying curl fallback...")

    # 2. Try curl
    url = f"https://github.com/{repo}/releases/download/{tag}/{asset_name}"
    curl_cmd = ["curl", "-fsSL", "-o", str(dest_path), url]
    if token:
        curl_cmd.extend(["-H", f"Authorization: token {token}"])

    curl_res = subprocess.run(curl_cmd, capture_output=True, text=True)
    if curl_res.returncode != 0 or not dest_path.is_file():
        raise RuntimeError(
            f"Failed to download release asset from {url}.\n"
            f"curl error: {curl_res.stderr.strip()}"
        )

    print("Download via curl succeeded.")
    return dest_path


def main():
    parser = argparse.ArgumentParser(description="Fetch and verify HCM-Traffic reference inputs.")
    parser.add_argument("--target-dir", type=Path, default=Path("data/reference"), help="Directory where reference inputs will be installed")
    parser.add_argument("--archive-path", type=Path, default=None, help="Explicit path to downloaded tar.gz archive")
    parser.add_argument("--release-tag", type=str, default="v1.0.0-inputs", help="GitHub Release tag")
    parser.add_argument("--repo", type=str, default=os.environ.get("GITHUB_REPOSITORY", "kienhuynh22/HCM-Traffic"))
    parser.add_argument("--expected-sha256", type=str, default="", help="Expected SHA-256 of the tar.gz package")
    parser.add_argument("--force", action="store_true", help="Force re-fetch and re-extract even if files already exist")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing installed files without downloading")
    args = parser.parse_args()

    target_dir = args.target_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    expected_archive_sha = (args.expected_sha256 or DEFAULT_EXPECTED_SHA).strip().lower()

    # 1. Quick check if already extracted and valid
    pkg_manifest_in_target = target_dir / "manifest.json"
    is_valid = check_existing_files(target_dir, pkg_manifest_in_target)
    if args.verify_only:
        if is_valid:
            print(f"Integrity verified: All reference files are present and match checksums in {target_dir}.")
            return 0
        else:
            print(f"Verification failed: Files in {target_dir} are missing, incomplete, or corrupted.", file=sys.stderr)
            return 1

    if not args.force and is_valid:
        print(f"Integrity verified: All reference files are already present and match checksums in {target_dir}.")
        return 0

    print("Reference inputs missing or incomplete. Locating input archive...")

    archive = None
    if args.archive_path:
        if not args.archive_path.is_file():
            print(f"ERROR: Specified --archive-path '{args.archive_path}' does not exist!", file=sys.stderr)
            return 1
        archive = args.archive_path.resolve()

    if not archive:
        local_dist = Path(f"dist/{DEFAULT_ASSET_NAME}").resolve()
        if local_dist.is_file():
            print(f"Found local distribution package at {local_dist}.")
            archive = local_dist

    if not archive:
        download_dir = Path(".local/tmp/downloads").resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
        try:
            archive = download_asset(args.repo, args.release_tag, DEFAULT_ASSET_NAME, download_dir)
        except Exception as exc:
            print(f"ERROR: Could not download reference archive: {exc}", file=sys.stderr)
            return 1

    # 2. Strict archive SHA-256 verification before touching anything
    print(f"Verifying package checksum for {archive}...")
    actual_archive_sha = sha256_file(archive)
    print(f"  Package SHA-256: {actual_archive_sha}")
    if actual_archive_sha != expected_archive_sha:
        print(
            f"ERROR: Package SHA-256 mismatch!\n"
            f"  Expected: {expected_archive_sha}\n"
            f"  Actual:   {actual_archive_sha}",
            file=sys.stderr,
        )
        return 1
    print("Package checksum verified successfully.")

    # 3. Extract into isolated staging directory with path traversal protection
    staging_dir = target_dir.parent / f"_staging_inputs_{os.getpid()}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Unpacking archive into staging directory: {staging_dir}...")
        with tarfile.open(archive, "r:gz") as tar:
            for member in tar.getmembers():
                if not is_safe_member(member, staging_dir):
                    raise SecurityError(f"Potentially malicious path in archive member: {member.name}")
            tar.extractall(path=staging_dir)

        # 4. Read embedded manifest and verify each individual file in staging
        embedded_manifest_path = staging_dir / "manifest.json"
        if not embedded_manifest_path.is_file():
            raise FileNotFoundError("Archive is missing embedded manifest.json.")

        manifest = json.loads(embedded_manifest_path.read_text(encoding="utf-8"))
        print(f"Verifying {len(manifest.get('files', {}))} individual files in staging...")
        for rel_path, meta in manifest.get("files", {}).items():
            staged_file = staging_dir / rel_path
            if not staged_file.is_file():
                raise FileNotFoundError(f"Missing staged file: {rel_path}")
            if staged_file.stat().st_size != meta["size_bytes"]:
                raise ValueError(f"Size mismatch on {rel_path}: expected {meta['size_bytes']}, got {staged_file.stat().st_size}")
            staged_sha = sha256_file(staged_file)
            if staged_sha != meta["sha256"]:
                raise ValueError(f"Checksum mismatch on {rel_path}: expected {meta['sha256']}, got {staged_sha}")

        print("All staged files successfully verified.")

        # 5. Transactional installation from staging to target_dir with rollback
        print(f"Installing reference files to final destination: {target_dir}...")
        backup_dir = target_dir.parent / f"_backup_ref_{os.getpid()}"
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)

        has_existing = target_dir.exists() and any(target_dir.iterdir())
        if has_existing:
            shutil.copytree(target_dir, backup_dir)

        try:
            for item in staging_dir.iterdir():
                dest = target_dir / item.name
                if item.is_dir():
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(item, dest)
                else:
                    shutil.copy2(item, dest)

            # Success: clean backup
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)
            print(f"SUCCESS: All reference inputs successfully installed and verified in {target_dir}.")
            return 0
        except Exception as install_err:
            print(f"CRITICAL: Installation failed midway: {install_err}. Initiating rollback...", file=sys.stderr)
            if backup_dir.exists():
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                shutil.copytree(backup_dir, target_dir)
                shutil.rmtree(backup_dir, ignore_errors=True)
                print("Rollback complete: previous target directory restored cleanly.", file=sys.stderr)
            raise
    except Exception as err:
        print(f"ERROR during extraction/verification: {err}", file=sys.stderr)
        return 1
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
