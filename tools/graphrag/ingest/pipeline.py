"""Restartable offline ingestion with bounded batches and disk-backed graph merging."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator

from .entity_graph import graph_record_stream, write_graph_artifact
from .normalize import ChunkingConfig, chunk_document, documents
from .sources.source_loader import SourceLoader, SourceManifest


class ImportFailure(RuntimeError):
    """Raised after an import is recorded as FAILED."""


class JsonlRecords:
    """Repeatable, sized iterable that opens a fresh stream for each builder pass."""

    def __init__(self, path: Path, count: int):
        self.path = path
        self.count = count

    def __len__(self) -> int:
        return self.count

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        with self.path.open(encoding="utf-8") as stream:
            for line in stream:
                yield json.loads(line)


def _write_record(output: Any, record: Dict[str, Any]) -> None:
    output.write(json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n")


def _merge_graph(connection: sqlite3.Connection, kind: str, record: Dict[str, Any]) -> None:
    key = record["vertex_id" if kind == "vertex" else "edge_id"]
    record = dict(record)
    for field in ("source_document_ids", "source_chunk_ids"):
        for reference in record.pop(field, []):
            connection.execute("INSERT OR IGNORE INTO provenance(kind,id,field,reference) VALUES (?,?,?,?)",
                               (kind, key, field, reference))
    row = connection.execute("SELECT payload FROM graph WHERE kind=? AND id=?", (kind, key)).fetchone()
    if row is not None:
        previous = json.loads(row[0])
        if kind == "vertex":
            previous["canonical_name"] = min(previous.get("canonical_name", ""),
                                             record.get("canonical_name", ""))
        record = previous
    connection.execute("INSERT OR REPLACE INTO graph(kind,id,payload) VALUES (?,?,?)",
                       (kind, key, json.dumps(record, sort_keys=True, ensure_ascii=False)))


def _graph_values(connection: sqlite3.Connection, kind: str) -> Iterator[Dict[str, Any]]:
    for key, payload in connection.execute("SELECT id,payload FROM graph WHERE kind=? ORDER BY id", (kind,)):
        value = json.loads(payload)
        fields = ("source_document_ids", "source_chunk_ids") if kind == "edge" else ("source_document_ids",)
        for field in fields:
            value[field] = [row[0] for row in connection.execute(
                "SELECT reference FROM provenance WHERE kind=? AND id=? AND field=? ORDER BY reference",
                (kind, key, field))]
        yield value


def run_import(manifest: SourceManifest, output_directory: Path, loader: SourceLoader,
               chunking: ChunkingConfig, bm25_builder: Callable[[Path, Iterable[Dict[str, Any]]], Path],
               vector_builder: Callable[[Path, Iterable[Dict[str, Any]]], Path]) -> Dict[str, Any]:
    """Publish one immutable version only after all builders finish reading canonical spills."""
    output_directory.mkdir(parents=True, exist_ok=True)
    identity = {"dataset": manifest.dataset, "dataset_release": manifest.dataset_release,
                "split": manifest.split, "sha256": manifest.sha256,
                "parser_version": manifest.parser_version, "policy_version": chunking.policy_version(),
                "pipeline_version": "disk-spill-v1", "source_uri": manifest.source_uri,
                "cache_uri": Path(manifest.cache_path).expanduser().resolve().as_uri()
                    if not manifest.source_uri and manifest.cache_path else None}
    version = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"))
                             .encode("utf-8")).hexdigest()[:16]
    final = output_directory / version
    if final.exists() and (final / "READY.json").is_file():
        return json.loads((final / "READY.json").read_text(encoding="utf-8"))
    staging = Path(tempfile.mkdtemp(prefix=version + ".", dir=output_directory))
    metadata: Dict[str, Any] = dict(identity, version=version, state="IMPORTING", errors=[])
    try:
        document_count = 0
        chunk_count = 0
        database_path = staging / "graph-staging.sqlite"
        connection = sqlite3.connect(database_path)
        try:
            connection.execute("PRAGMA cache_size=-2048")
            connection.execute("CREATE TABLE graph(kind TEXT, id TEXT, payload TEXT, PRIMARY KEY(kind,id))")
            connection.execute("CREATE TABLE provenance(kind TEXT, id TEXT, field TEXT, reference TEXT, "
                               "PRIMARY KEY(kind,id,field,reference))")
            connection.execute("CREATE TABLE documents(id TEXT PRIMARY KEY)")
            with (staging / "documents.jsonl").open("w", encoding="utf-8") as document_output, \
                 (staging / "chunks.jsonl").open("w", encoding="utf-8") as chunk_output:
                for batch in loader.load_batches(manifest):
                    for record in batch:
                        connection.execute("INSERT INTO documents(id) VALUES (?)", (record["document_id"],))
                        document = next(documents([record]))
                        if not document.get("source_uri"):
                            raise ImportFailure("document source URI is missing")
                        _write_record(document_output, document)
                        document_count += 1
                        document_chunks = chunk_document(document, chunking)
                        for chunk in document_chunks:
                            _write_record(chunk_output, chunk)
                            chunk_count += 1
                        # Only one document's chunks are resident; merged graph records live on disk.
                        for vertex, edge in graph_record_stream([record], document_chunks):
                            if vertex is not None:
                                _merge_graph(connection, "vertex", vertex)
                            if edge is not None:
                                _merge_graph(connection, "edge", edge)
                    connection.commit()
            write_graph_artifact(staging, "graph", _graph_values(connection, "vertex"),
                                 _graph_values(connection, "edge"))
        finally:
            connection.close()
            database_path.unlink(missing_ok=True)
        graph_manifest = json.loads((staging / "graph/manifest.json").read_text(encoding="utf-8"))
        metadata["state"] = "INDEXING"
        canonical_chunks = JsonlRecords(staging / "chunks.jsonl", chunk_count)
        bm25_path = bm25_builder(staging, canonical_chunks)
        vector_path = vector_builder(staging, canonical_chunks)
        metadata.update({
            "state": "READY", "documents": document_count, "chunks": chunk_count,
            "vertices": graph_manifest["vertex_count"], "edges": graph_manifest["edge_count"],
            "graph_manifest": "graph/manifest.json",
            "bm25_artifact": str(Path(bm25_path).relative_to(staging)),
            "vector_artifact": str(Path(vector_path).relative_to(staging)),
        })
        for relative in (metadata["graph_manifest"], metadata["bm25_artifact"], metadata["vector_artifact"]):
            candidate = staging / relative
            if not candidate.exists() or staging.resolve() not in candidate.resolve().parents:
                raise ImportFailure("artifact is missing or outside staging: %s" % relative)
        (staging / "READY.json").write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n",
                                           encoding="utf-8")
        # The target is a nonempty immutable directory; rename cannot replace a READY winner.
        try:
            os.rename(staging, final)
        except OSError:
            if not (final / "READY.json").is_file():
                raise
            winner = json.loads((final / "READY.json").read_text(encoding="utf-8"))
            if winner != metadata:
                raise ImportFailure("existing import metadata conflicts with this attempt")
            return winner
        return metadata
    except Exception as failure:
        metadata["state"] = "FAILED"
        metadata["errors"] = [str(failure)]
        (output_directory / (staging.name + ".FAILED.json")).write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        raise ImportFailure("import failed for %s/%s: %s" %
                            (manifest.dataset, manifest.split, failure)) from failure
    finally:
        shutil.rmtree(staging, ignore_errors=True)
