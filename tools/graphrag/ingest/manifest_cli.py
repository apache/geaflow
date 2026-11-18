"""Manifest validation entry point for reproducible offline dataset imports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from .digests import sha256_file
from .sources.source_loader import SourceLoader, SourceManifest


def resolve_manifest(path: Path, output: Path, loader: Optional[SourceLoader] = None) -> Dict[str, Any]:
    """Resolve a manifest against a verified local cache and write an auditable copy."""
    manifest = SourceManifest.from_file(path)
    active_loader = loader or SourceLoader()
    source = active_loader._resolve_source(manifest)
    digest = sha256_file(source)
    if digest != manifest.sha256:
        # Keep the loader's canonical checksum error and avoid writing an unverified resolution.
        active_loader._verify_checksum(source, manifest)
    resolved = json.loads(path.read_text(encoding="utf-8"))
    resolved["sha256"] = digest
    resolved["resolved_cache_path"] = str(source.resolve())
    resolved["resolved_source_uri"] = source.resolve().as_uri()
    resolved["resolved_from"] = str(path.resolve())
    resolved["state"] = "RESOLVED"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(resolved, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return resolved


def dry_run_manifest(path: Path, loader: Optional[SourceLoader] = None) -> Dict[str, Any]:
    """Verify license metadata, cache checksum, split selection, and record parsing."""
    manifest = SourceManifest.from_file(path)
    active_loader = loader or SourceLoader()
    records = 0
    for batch in active_loader.load_batches(manifest):
        records += len(batch)
    return {
        "manifest": str(path),
        "dataset": manifest.dataset,
        "dataset_release": manifest.dataset_release,
        "split": manifest.split,
        "records": records,
        "sha256": manifest.sha256,
        "license": {"url": manifest.license_url, "status": manifest.license_status},
        "state": "VALIDATED",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a GraphRAG dataset manifest and cache")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--allow-download", action="store_true")
    parser.add_argument("--resolve-output", type=Path)
    args = parser.parse_args()
    loader = SourceLoader(allow_download=args.allow_download)
    if args.resolve_output:
        result = resolve_manifest(args.manifest, args.resolve_output, loader)
    else:
        result = dry_run_manifest(args.manifest, loader)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
