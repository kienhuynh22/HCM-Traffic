"""
Package builder for HCM-Traffic offline reference inputs.
Packages candidates bundle, graph cache, capture plans, and region PBF into a single
self-contained, verified archive with embedded manifest and SHA-256 checksums.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile
import time

PACKAGE_VERSION = "main13_inputs_v1"
OUTPUT_TAR = Path(f"dist/hcm_traffic_inputs_{PACKAGE_VERSION}.tar.gz")
OUTPUT_SHA = Path(f"dist/hcm_traffic_inputs_{PACKAGE_VERSION}.tar.gz.sha256")
OUTPUT_MANIFEST = Path(f"dist/hcm_traffic_inputs_{PACKAGE_VERSION}_manifest.json")

FILES_TO_PACKAGE = [
    # Candidate bundle
    ("data/reference/candidates/main13_20261117_869a0f5a/manifest.json", "candidates/main13_20261117_869a0f5a/manifest.json"),
    ("data/reference/candidates/main13_20261117_869a0f5a/paths.json", "candidates/main13_20261117_869a0f5a/paths.json"),
    # Graph cache
    ("data/reference/graph/09763400d0887076fc48/manifest.json", "graph/09763400d0887076fc48/manifest.json"),
    ("data/reference/graph/09763400d0887076fc48/graph.json", "graph/09763400d0887076fc48/graph.json"),
    ("data/reference/graph/09763400d0887076fc48/graph.sqlite", "graph/09763400d0887076fc48/graph.sqlite"),
    ("data/reference/graph/09763400d0887076fc48/normalized.sqlite", "graph/09763400d0887076fc48/normalized.sqlite"),
    # Capture plans
    ("data/reference/plans/38b28d4596ca3dc3d4ea.json", "plans/38b28d4596ca3dc3d4ea.json"),
    ("data/reference/plans/43da41515fbf56fd910e.json", "plans/43da41515fbf56fd910e.json"),
    # Regional OSM PBF
    ("data/reference/6fe4fd15df6bb8c5453d/region.osm.pbf", "6fe4fd15df6bb8c5453d/region.osm.pbf"),
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Package HCM-Traffic reference inputs.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing package if already present")
    args = parser.parse_args()

    if OUTPUT_TAR.is_file() and not args.force:
        print(f"Package already exists at {OUTPUT_TAR}. Use --force to rebuild.")
        return 0

    OUTPUT_TAR.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "package_version": PACKAGE_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": {},
    }

    print("Verifying source files and computing individual SHA-256 hashes...")
    for src_rel, arcname in FILES_TO_PACKAGE:
        src_path = Path(src_rel)
        if not src_path.is_file():
            raise FileNotFoundError(f"Required input file missing: {src_path}")
        h = sha256_file(src_path)
        size = src_path.stat().st_size
        manifest["files"][arcname] = {"sha256": h, "size_bytes": size}
        print(f"  + {arcname} ({size / (1024*1024):.2f} MB, {h[:12]}...)")

    staging_manifest = OUTPUT_TAR.parent / f"_staging_manifest_{os.getpid()}.json"
    staging_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"\nCompressing into {OUTPUT_TAR} ...")
    t0 = time.time()
    with tarfile.open(OUTPUT_TAR, "w:gz") as tar:
        # Embed manifest inside archive as top-level reference/manifest.json
        tar.add(staging_manifest, arcname="manifest.json")
        for src_rel, arcname in FILES_TO_PACKAGE:
            src_path = Path(src_rel)
            tar.add(src_path, arcname=arcname)
    elapsed = time.time() - t0
    staging_manifest.unlink()

    tar_size_mb = OUTPUT_TAR.stat().st_size / (1024 * 1024)
    tar_sha = sha256_file(OUTPUT_TAR)
    manifest["package_sha256"] = tar_sha
    manifest["package_size_bytes"] = OUTPUT_TAR.stat().st_size

    OUTPUT_SHA.write_text(f"{tar_sha}  {OUTPUT_TAR.name}\n", encoding="utf-8")
    with open(OUTPUT_MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nPackage created successfully in {elapsed:.1f}s:")
    print(f"  Archive:  {OUTPUT_TAR} ({tar_size_mb:.2f} MB)")
    print(f"  SHA-256:  {tar_sha}")
    print(f"  Checksum: {OUTPUT_SHA}")
    print(f"  Manifest: {OUTPUT_MANIFEST}")
    return 0


if __name__ == "__main__":
    from datetime import datetime, timezone
    sys.exit(main())
