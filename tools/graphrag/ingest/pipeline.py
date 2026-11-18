"""Restartable offline ingestion with bounded batches and disk-backed graph merging."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Optional

from .entity_graph import (EXTRACTOR_VERSION, GRAPH_SCHEMA_VERSION, aggregate_source_hash,
                           graph_record_stream, write_graph_artifact)
from .digests import sha256_file
from .normalize import NORMALIZATION_VERSION, ChunkingConfig, chunk_document, documents
from .sources.source_loader import SourceLoader, SourceManifest


class ImportFailure(RuntimeError):
    """Raised after an import is recorded as FAILED."""


class ArtifactBuildResult:
    """Verified builder output and the canonical chunks represented by the artifact."""

    def __init__(self, path: Path, covered_chunk_count: int, covered_chunk_sha256: str):
        self.path = Path(path)
        self.covered_chunk_count = covered_chunk_count
        self.covered_chunk_sha256 = covered_chunk_sha256

    @classmethod
    def from_chunk_ids(cls, path: Path, chunk_ids: Iterable[str]) -> "ArtifactBuildResult":
        ordered = sorted(set(chunk_ids))
        digest = hashlib.sha256()
        for chunk_id in ordered:
            value = chunk_id.encode("utf-8")
            digest.update(str(len(value)).encode("ascii") + b":" + value)
        return cls(path, len(ordered), digest.hexdigest())


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


def _artifact_checksums(root: Path, artifact: Path) -> Dict[str, str]:
    paths = sorted(path for path in ([artifact] if artifact.is_file() else artifact.rglob("*"))
                   if path.is_file())
    if not paths:
        raise ImportFailure("artifact is empty: %s" % artifact)
    for path in paths:
        if root.resolve() not in path.resolve().parents:
            raise ImportFailure("artifact contains a file outside release: %s" % path)
    return {str(path.relative_to(root)): sha256_file(path) for path in paths}


def _validate_ready_artifacts(root: Path, metadata: Dict[str, Any]) -> None:
    checksums = metadata.get("artifact_checksums")
    if not isinstance(checksums, dict) or not checksums:
        raise ImportFailure("existing READY metadata has no artifact checksums")
    root = root.resolve()
    required = {"documents.jsonl", "chunks.jsonl", metadata.get("graph_manifest"),
                metadata.get("quality_report"), metadata.get("source_manifest")}
    artifact_keys = [key for key in ("bm25_artifact", "vector_artifact") if key in metadata]
    if len(artifact_keys) == 1:
        raise ImportFailure("existing READY metadata has incomplete native index metadata")
    for key in artifact_keys:
        artifact_path = metadata.get(key)
        if not isinstance(artifact_path, str) or not artifact_path:
            raise ImportFailure("existing READY metadata is missing %s" % key)
        resolved_artifact = (root / artifact_path).resolve()
        if root not in resolved_artifact.parents or not resolved_artifact.exists():
            raise ImportFailure("existing READY artifact is missing or outside release: %s"
                                % artifact_path)
        if resolved_artifact.is_dir():
            required.update(str(path.relative_to(root)) for path in resolved_artifact.rglob("*")
                            if path.is_file())
        else:
            required.add(artifact_path)
    if not required.issubset(checksums):
        raise ImportFailure("existing READY checksum manifest omits required artifacts")
    actual = {str(path.relative_to(root)) for path in root.rglob("*")
              if path.is_file() and path.name != "READY.json"}
    if actual != set(checksums):
        raise ImportFailure("existing READY checksum manifest does not match release files")
    for relative, expected in checksums.items():
        candidate = (root / relative).resolve()
        if root not in candidate.parents or not candidate.is_file():
            raise ImportFailure("existing READY artifact is missing or outside release: %s" % relative)
        if sha256_file(candidate) != expected:
            raise ImportFailure("existing READY artifact checksum mismatch: %s" % relative)


def _graph_quality(root: Path, graph_manifest: Dict[str, Any], documents: int,
                   chunks: int, connection: sqlite3.Connection,
                   quarantined_records: int) -> Dict[str, Any]:
    missing_provenance = {"vertices": 0, "edges": 0}
    consistency = {
        "missing_entity_chunks": 0,
        "missing_edge_chunks": 0,
        "unknown_chunk_references": 0,
        "document_chunk_mismatches": 0,
        "source_hash_mismatches": 0,
        "dangling_edge_endpoints": 0,
        "unknown_document_references": 0,
    }
    graph_root = root / "graph" / graph_manifest["graph_version"]
    for kind in ("vertices", "edges"):
        path = graph_root / (kind + ".jsonl")
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                value = json.loads(line)
                source_chunks = value.get("source_chunk_ids") or []
                source_documents = value.get("source_document_ids") or []
                missing = (not value.get("dataset") or not value.get("dataset_release")
                           or not value.get("split") or not value.get("source_hash")
                           or not source_documents or not source_chunks)
                missing_provenance[kind] += int(missing)
                if kind == "vertices" and value.get("vertex_type") == "ENTITY" and not source_chunks:
                    consistency["missing_entity_chunks"] += 1
                if kind == "edges" and not source_chunks:
                    consistency["missing_edge_chunks"] += 1
                source_hashes = []
                for document_id in source_documents:
                    if not connection.execute(
                            "SELECT 1 FROM documents WHERE id=?", (document_id,)
                    ).fetchone():
                        consistency["unknown_document_references"] += 1
                for chunk_id in source_chunks:
                    row = connection.execute(
                        "SELECT document_id, source_hash FROM chunks WHERE id=?", (chunk_id,)
                    ).fetchone()
                    if row is None:
                        consistency["unknown_chunk_references"] += 1
                        continue
                    if row[0] not in source_documents:
                        consistency["document_chunk_mismatches"] += 1
                    source_hashes.append(row[1])
                if source_hashes and aggregate_source_hash(source_hashes) != value.get("source_hash"):
                    consistency["source_hash_mismatches"] += 1
                if kind == "edges":
                    for field in ("source_vertex_id", "target_vertex_id"):
                        endpoint = value.get(field)
                        if not connection.execute(
                                "SELECT 1 FROM graph WHERE kind='vertex' AND id=?", (endpoint,)
                        ).fetchone():
                            consistency["dangling_edge_endpoints"] += 1
    return {
        "documents": documents,
        "chunks": chunks,
        "vertices": graph_manifest["vertex_count"],
        "edges": graph_manifest["edge_count"],
        "missing_provenance": missing_provenance,
        "entity_chunk_consistency": consistency,
        "quarantined_records": quarantined_records,
        "index_coverage": {},
    }


def _chunk_fingerprint(connection: sqlite3.Connection) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for (chunk_id,) in connection.execute("SELECT id FROM chunks ORDER BY id"):
        value = chunk_id.encode("utf-8")
        digest.update(str(len(value)).encode("ascii") + b":" + value)
        count += 1
    return count, digest.hexdigest()


def _verified_build(result: Any, kind: str, connection: sqlite3.Connection,
                    staging: Path) -> ArtifactBuildResult:
    if not isinstance(result, ArtifactBuildResult):
        raise ImportFailure("%s builder must return ArtifactBuildResult" % kind)
    expected_count, expected_fingerprint = _chunk_fingerprint(connection)
    if (result.covered_chunk_count != expected_count
            or result.covered_chunk_sha256 != expected_fingerprint):
        raise ImportFailure("%s artifact chunk coverage does not match canonical chunks" % kind)
    path = result.path.resolve()
    if staging.resolve() not in path.parents or not path.exists():
        raise ImportFailure("%s artifact is missing or outside staging" % kind)
    if path.is_file() and path.stat().st_size == 0:
        raise ImportFailure("%s artifact is empty" % kind)
    if path.is_dir() and not any(child.is_file() for child in path.rglob("*")):
        raise ImportFailure("%s artifact is empty" % kind)
    return result


def _merge_graph(connection: sqlite3.Connection, kind: str, record: Dict[str, Any]) -> None:
    key = record["vertex_id" if kind == "vertex" else "edge_id"]
    record = dict(record)
    for field in ("source_document_ids", "source_chunk_ids"):
        for reference in record.pop(field, []):
            connection.execute("INSERT OR IGNORE INTO provenance(kind,id,field,reference) VALUES (?,?,?,?)",
                               (kind, key, field, reference))
    for source_hash in record.pop("source_hashes", []):
        connection.execute("INSERT OR IGNORE INTO provenance(kind,id,field,reference) VALUES (?,?,?,?)",
                           (kind, key, "source_hashes", source_hash))
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
        fields = ("source_document_ids", "source_chunk_ids", "source_hashes")
        for field in fields:
            references = [row[0] for row in connection.execute(
                "SELECT reference FROM provenance WHERE kind=? AND id=? AND field=? ORDER BY reference",
                (kind, key, field))]
            if field == "source_hashes":
                from .entity_graph import aggregate_source_hash
                value["source_hash"] = aggregate_source_hash(references)
            else:
                value[field] = references
        yield value


def run_import(manifest: SourceManifest, output_directory: Path, loader: SourceLoader,
               chunking: ChunkingConfig,
               bm25_builder: Optional[Callable[[Path, Iterable[Dict[str, Any]]], ArtifactBuildResult]] = None,
               vector_builder: Optional[Callable[[Path, Iterable[Dict[str, Any]]], ArtifactBuildResult]] = None,
               vector_source: str = "unknown", vector_version: str = "unknown",
               random_seed: int = 0, entity_schema_version: str = GRAPH_SCHEMA_VERSION,
               entity_extractor_version: Optional[str] = None) -> Dict[str, Any]:
    """Publish one immutable version only after all builders finish reading canonical spills."""
    output_directory.mkdir(parents=True, exist_ok=True)
    extractor_version = entity_extractor_version or EXTRACTOR_VERSION
    if (bm25_builder is None) != (vector_builder is None):
        raise ValueError("BM25 and vector builders must either both be configured or both be omitted")
    identity = {"dataset": manifest.dataset, "dataset_release": manifest.dataset_release,
                "split": manifest.split, "sha256": manifest.sha256,
                "parser_version": manifest.parser_version, "policy_version": chunking.policy_version(),
                "pipeline_version": "disk-spill-v1", "source_uri": manifest.source_uri,
                "cache_uri": Path(manifest.cache_path).expanduser().resolve().as_uri()
                    if not manifest.source_uri and manifest.cache_path else None,
                "vector_source": vector_source, "vector_version": vector_version,
                "random_seed": random_seed, "entity_schema_version": entity_schema_version,
                "entity_extractor_version": extractor_version,
                "preprocessing_version": NORMALIZATION_VERSION,
                "license_url": manifest.license_url, "license_status": manifest.license_status}
    version = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"))
                             .encode("utf-8")).hexdigest()[:16]
    final = output_directory / version
    if final.exists() and (final / "READY.json").is_file():
        existing = json.loads((final / "READY.json").read_text(encoding="utf-8"))
        if existing.get("state") != "READY" or existing.get("version") != version:
            raise ImportFailure("existing release metadata is not READY for its version directory")
        if any(existing.get(key) != value for key, value in identity.items()):
            raise ImportFailure("existing READY metadata conflicts with this import")
        _validate_ready_artifacts(final, existing)
        return existing
    staging = Path(tempfile.mkdtemp(prefix=version + ".", dir=output_directory))
    metadata: Dict[str, Any] = dict(identity, version=version, state="IMPORTING", errors=[])
    resolved_cache = None
    if manifest.cache_path:
        candidate = Path(os.path.expandvars(manifest.cache_path)).expanduser()
        if candidate.is_file():
            resolved_cache = str(candidate.resolve())
    metadata["source_manifest"] = "source-manifest.json"
    metadata["source_provenance"] = {
        "dataset": manifest.dataset,
        "dataset_release": manifest.dataset_release,
        "split": manifest.split,
        "cache_path": manifest.cache_path,
        "official_source_uri": manifest.source_uri,
        "resolved_cache_path": resolved_cache,
        "resolved_cache_uri": Path(resolved_cache).as_uri() if resolved_cache else None,
        "sha256": manifest.sha256,
        "parser_version": manifest.parser_version,
        "license_url": manifest.license_url,
        "license_status": manifest.license_status,
    }
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
            connection.execute("CREATE TABLE chunks(id TEXT PRIMARY KEY, document_id TEXT, "
                               "source_hash TEXT)")
            graph_stats: Dict[str, int] = {"quarantined_records": 0}
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
                            connection.execute("INSERT INTO chunks(id,document_id,source_hash) "
                                               "VALUES (?,?,?)", (chunk["chunk_id"],
                                               chunk["document_id"], chunk["source_hash"]))
                            chunk_count += 1
                        # Only one document's chunks are resident; merged graph records live on disk.
                        for vertex, edge in graph_record_stream([record], document_chunks, graph_stats):
                            if vertex is not None:
                                _merge_graph(connection, "vertex", vertex)
                            if edge is not None:
                                _merge_graph(connection, "edge", edge)
                    connection.commit()
            write_graph_artifact(staging / "graph", version, _graph_values(connection, "vertex"),
                                 _graph_values(connection, "edge"), entity_schema_version,
                                 extractor_version)
            graph_manifest_path = staging / "graph" / version / "manifest.json"
            graph_manifest = json.loads(graph_manifest_path.read_text(encoding="utf-8"))
            metadata["state"] = "INDEXING"
            canonical_chunks = JsonlRecords(staging / "chunks.jsonl", chunk_count)
            bm25_result = None
            vector_result = None
            if bm25_builder is not None:
                bm25_result = _verified_build(
                    bm25_builder(staging, canonical_chunks), "bm25", connection, staging)
                vector_result = _verified_build(
                    vector_builder(staging, canonical_chunks), "vector", connection, staging)
            quality = _graph_quality(staging, graph_manifest, document_count, chunk_count,
                                     connection, graph_stats["quarantined_records"])
            quality["index_coverage"] = {}
            if bm25_result is not None and vector_result is not None:
                quality["index_coverage"] = {
                    "bm25": {"covered_chunks": bm25_result.covered_chunk_count,
                             "total_chunks": chunk_count, "ratio": 1.0 if chunk_count else 0.0},
                    "vector": {"covered_chunks": vector_result.covered_chunk_count,
                               "total_chunks": chunk_count, "ratio": 1.0 if chunk_count else 0.0},
                }
        finally:
            connection.close()
            database_path.unlink(missing_ok=True)
        metadata.update({
            "state": "READY", "documents": document_count, "chunks": chunk_count,
            "vertices": graph_manifest["vertex_count"], "edges": graph_manifest["edge_count"],
            "graph_manifest": str(graph_manifest_path.relative_to(staging)),
        })
        if bm25_result is not None and vector_result is not None:
            metadata["bm25_artifact"] = str(bm25_result.path.relative_to(staging))
            metadata["vector_artifact"] = str(vector_result.path.relative_to(staging))
        (staging / metadata["source_manifest"]).write_text(
            json.dumps(metadata["source_provenance"], sort_keys=True, indent=2) + "\n",
            encoding="utf-8")
        required_artifacts = [metadata["graph_manifest"]]
        if bm25_result is not None and vector_result is not None:
            required_artifacts.extend((metadata["bm25_artifact"], metadata["vector_artifact"]))
        for relative in required_artifacts:
            candidate = staging / relative
            if not candidate.exists() or staging.resolve() not in candidate.resolve().parents:
                raise ImportFailure("artifact is missing or outside staging: %s" % relative)
        metadata["quality_report"] = "quality-report.json"
        metadata["artifact_checksums"] = {}
        graph_root = staging / "graph" / version
        artifacts = [staging / "documents.jsonl", staging / "chunks.jsonl",
                     staging / metadata["source_manifest"], graph_root]
        if bm25_result is not None and vector_result is not None:
            artifacts.extend((bm25_result.path, vector_result.path))
        for artifact in artifacts:
            metadata["artifact_checksums"].update(_artifact_checksums(staging, artifact))
        (staging / metadata["quality_report"]).write_text(
            json.dumps(quality, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        metadata["artifact_checksums"].update(
            _artifact_checksums(staging, staging / metadata["quality_report"]))
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
