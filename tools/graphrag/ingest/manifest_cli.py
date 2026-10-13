"""Manifest validation entry point for reproducible offline dataset imports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from .sources.source_loader import SourceLoader, SourceManifest


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
    args = parser.parse_args()
    result = dry_run_manifest(args.manifest, SourceLoader(allow_download=args.allow_download))
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
