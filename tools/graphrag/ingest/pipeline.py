"""Restartable offline ingestion orchestration for the Week 2 artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from .entity_graph import graph_records, write_graph_artifact
from .normalize import ChunkingConfig, chunks, documents
from .sources.source_loader import SourceLoader, SourceManifest


class ImportFailure(RuntimeError):
    """Raised after an import is recorded as FAILED."""


def run_import(manifest: SourceManifest, output_directory: Path, loader: SourceLoader,
               chunking: ChunkingConfig, bm25_builder: Callable[[Path, List[Dict[str, Any]]], Path],
               vector_builder: Callable[[Path, List[Dict[str, Any]]], Path]) -> Dict[str, Any]:
    """Build one immutable version; publish the metadata file only after every artifact succeeds."""
    output_directory.mkdir(parents=True, exist_ok=True)
    version = hashlib.sha256((manifest.dataset + "\0" + manifest.split + "\0" + manifest.sha256
                             + "\0" + manifest.parser_version + "\0" + repr(chunking))
                             .encode("utf-8")).hexdigest()[:16]
    final = output_directory / version
    if final.exists() and (final / "READY.json").is_file():
        return json.loads((final / "READY.json").read_text(encoding="utf-8"))
    staging = Path(tempfile.mkdtemp(prefix=version + ".", dir=output_directory))
    metadata: Dict[str, Any] = {
        "dataset": manifest.dataset, "dataset_release": manifest.dataset_release,
        "split": manifest.split, "sha256": manifest.sha256, "version": version,
        "state": "IMPORTING", "parser_version": manifest.parser_version,
        "errors": [],
    }
    try:
        source_records = list(loader.load(manifest))
        normalized = list(documents(source_records))
        normalized_chunks = list(chunks(normalized, chunking))
        (staging / "chunks.jsonl").write_text(
            "".join(json.dumps(item, sort_keys=True, ensure_ascii=False) + "\n"
                    for item in normalized_chunks), encoding="utf-8")
        vertices, edges = graph_records(source_records, normalized_chunks)
        graph_manifest = write_graph_artifact(staging, "graph", vertices, edges)
        metadata["state"] = "INDEXING"
        bm25_path = bm25_builder(staging, normalized_chunks)
        vector_path = vector_builder(staging, normalized_chunks)
        metadata.update({
            "state": "READY", "documents": len(normalized), "chunks": len(normalized_chunks),
            "vertices": len(vertices), "edges": len(edges), "graph_manifest": "graph/manifest.json",
            "bm25_artifact": Path(bm25_path).name, "vector_artifact": Path(vector_path).name,
        })
        for relative in (metadata["graph_manifest"], metadata["bm25_artifact"], metadata["vector_artifact"]):
            if not (staging / relative).exists():
                raise ImportFailure("artifact does not exist before publication: %s" % relative)
        ready_path = staging / "READY.json"
        ready_path.write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(staging, final)
        return metadata
    except Exception as failure:
        metadata["state"] = "FAILED"
        metadata["errors"] = [str(failure)]
        (staging / "FAILED.json").write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n",
                                              encoding="utf-8")
        raise ImportFailure("import failed for %s/%s: %s" %
                            (manifest.dataset, manifest.split, failure)) from failure
