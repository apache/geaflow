"""Deterministic dataset-evidence entity mapping and graph JSONL writer."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple


EXTRACTOR_VERSION = "text-cooccurrence-entity-v2"
GRAPH_SCHEMA_VERSION = "chunk-entity-cooccurrence-v2"
SUPPORTED_RELATIONS = frozenset({
    "supports", "related to", "relates to", "country", "country of origin",
    "country of citizenship", "mentions", "cooccurs",
})

# A canonical chunk can contain many page titles.  Pairing every title in a
# large chunk creates a quadratic graph and gives no additional retrieval
# signal after the first few neighbors, so retain a deterministic prefix.
MAX_COOCCURRENCE_ENTITIES = 32


def canonical_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return " ".join(normalized.casefold().split())


def stable_id(kind: str, *parts: str) -> str:
    payload = "\0".join((kind,) + parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def aggregate_source_hash(values: Iterable[str]) -> str:
    hashes = sorted({value for value in values if value})
    if not hashes:
        return ""
    if len(hashes) == 1:
        return hashes[0]
    identity = "".join(str(len(value)) + ":" + value for value in hashes)
    return "sha256:aggregate:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _entity(name: str, dataset: str, release: str) -> Dict[str, Any]:
    normalized = canonical_name(name)
    return {
        "entity_id": stable_id("entity", dataset, release, normalized),
        "canonical_name": " ".join(unicodedata.normalize("NFC", name).split()),
        "normalized_name": normalized,
        "entity_type": "CONCEPT",
    }


def graph_record_stream(records: Iterable[Dict[str, Any]],
                        chunks: Iterable[Dict[str, Any]],
                        stats: Dict[str, int] = None
                        ) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any]]]:
    stats = stats if stats is not None else {}
    stats.setdefault("quarantined_records", 0)
    chunk_list = list(chunks)
    by_document: Dict[str, List[Dict[str, Any]]] = {}
    for chunk in chunk_list:
        by_document.setdefault(chunk["document_id"], []).append(chunk)
    for record in records:
        dataset = record["dataset"]
        release = record["dataset_release"]
        split = record.get("split", "")
        document_hash = record.get("source_hash", "")
        document_id = record["document_id"]
        document_chunks = by_document.get(document_id, [])
        chunk_entities: Dict[str, set] = {}
        for paragraph in record.get("paragraphs", []):
            name = paragraph.get("title", "").strip()
            if not name:
                continue
            entity = _entity(name, dataset, release)
            matching_chunks = [chunk["chunk_id"] for chunk in document_chunks
                               if canonical_name(name) in canonical_name(chunk["text"])]
            if not matching_chunks:
                stats["quarantined_records"] += 1
                continue
            entity_vertex = dict(entity, vertex_id=entity["entity_id"],
                vertex_type="ENTITY", dataset=dataset, dataset_release=release,
                split=split, source_hash=document_hash, source_document_ids=[document_id],
                source_chunk_ids=matching_chunks, source_hashes=[document_hash])
            yield (entity_vertex, None)
            for chunk in document_chunks:
                if canonical_name(name) in canonical_name(chunk["text"]):
                    chunk_entities.setdefault(chunk["chunk_id"], set()).add(entity["entity_id"])
        for chunk in document_chunks:
            vertex_id = stable_id("chunk-vertex", chunk["chunk_id"])
            chunk_vertex = {
                "vertex_id": vertex_id, "vertex_type": "CHUNK", "chunk_id": chunk["chunk_id"],
                "document_id": document_id, "dataset": dataset,
                "dataset_release": release, "split": split,
                "source_hash": chunk.get("source_hash", document_hash),
                "source_document_ids": [document_id], "source_chunk_ids": [chunk["chunk_id"]],
                "source_hashes": [chunk.get("source_hash", document_hash)],
            }
            yield (chunk_vertex, None)
            for entity_id in sorted(chunk_entities.get(chunk["chunk_id"], set())):
                edge_id = stable_id("mentions", vertex_id, entity_id)
                yield (None, {
                    "edge_id": edge_id, "source_vertex_id": vertex_id,
                    "target_vertex_id": entity_id, "relation_type": "mentions",
                    "source_chunk_ids": [chunk["chunk_id"]], "source_document_ids": [document_id],
                    "dataset": dataset, "dataset_release": release, "split": split,
                    "source_hash": chunk.get("source_hash", document_hash),
                    "source_hashes": [chunk.get("source_hash", document_hash)],
                })
            # Build an unsupervised entity graph from entities that occur in
            # the same canonical text chunk.  This is deliberately derived
            # from text co-occurrence rather than supporting-fact annotations
            # so graph retrieval does not leak evaluation labels.
            cooccurring = sorted(chunk_entities.get(chunk["chunk_id"], set()))
            if len(cooccurring) > MAX_COOCCURRENCE_ENTITIES:
                cooccurring = cooccurring[:MAX_COOCCURRENCE_ENTITIES]
            for index, source_entity_id in enumerate(cooccurring):
                for target_entity_id in cooccurring[index + 1:]:
                    edge_id = stable_id("cooccurs", source_entity_id,
                                       target_entity_id, chunk["chunk_id"])
                    yield (None, {
                        "edge_id": edge_id, "source_vertex_id": source_entity_id,
                        "target_vertex_id": target_entity_id, "relation_type": "cooccurs",
                        "source_chunk_ids": [chunk["chunk_id"]],
                        "source_document_ids": [document_id], "dataset": dataset,
                        "dataset_release": release, "split": split,
                        "source_hash": chunk.get("source_hash", document_hash),
                        "source_hashes": [chunk.get("source_hash", document_hash)],
                    })
        # Supporting facts are evaluation annotations, not extracted graph
        # relations.  Only an explicit extractor output may add a relation;
        # otherwise the text-derived cooccurrence graph above is used.
        facts = record.get("evidences", []) or record.get("annotations", {}).get("evidences", [])
        for fact in facts:
            if not isinstance(fact, (list, tuple)) or len(fact) < 2:
                continue
            if len(fact) >= 3:
                subject, relation, object_name = (str(fact[0]), str(fact[1]), str(fact[2]))
            else:
                subject, relation, object_name = str(fact[0]), "SUPPORTS", document_id
            relation = canonical_name(relation)
            if not subject.strip() or not object_name.strip() or relation not in SUPPORTED_RELATIONS:
                continue
            source_entity = _entity(subject, dataset, release)
            target_entity = _entity(object_name, dataset, release)
            matching = [chunk["chunk_id"] for chunk in document_chunks
                        if canonical_name(subject) in canonical_name(chunk["text"])]
            if not matching:
                stats["quarantined_records"] += 1
                continue
            for entity in (source_entity, target_entity):
                yield (dict(entity, vertex_id=entity["entity_id"],
                    vertex_type="ENTITY", dataset=dataset, dataset_release=release,
                    split=split, source_hash=document_hash, source_document_ids=[document_id],
                    source_chunk_ids=matching, source_hashes=[document_hash]), None)
            source_chunk = matching[0] if matching else ""
            edge_id = stable_id("relation", source_entity["entity_id"], canonical_name(relation),
                                target_entity["entity_id"], source_chunk)
            yield (None, {
                "edge_id": edge_id, "source_vertex_id": source_entity["entity_id"],
                "target_vertex_id": target_entity["entity_id"], "relation_type": canonical_name(relation),
                "source_chunk_ids": [source_chunk] if source_chunk else [],
                "source_document_ids": [document_id], "dataset": dataset,
                "dataset_release": release, "split": split,
                "source_hash": aggregate_source_hash(
                    [chunk.get("source_hash", document_hash) for chunk in document_chunks
                     if chunk["chunk_id"] == source_chunk]),
                "source_hashes": [chunk.get("source_hash", document_hash)
                                  for chunk in document_chunks if chunk["chunk_id"] == source_chunk],
            })


def graph_records(records: Iterable[Dict[str, Any]],
                  chunks: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    vertices: Dict[str, Dict[str, Any]] = {}
    edges: Dict[str, Dict[str, Any]] = {}
    for vertex, edge in graph_record_stream(records, chunks):
        if vertex is not None:
            existing = vertices.get(vertex["vertex_id"])
            if existing is None:
                vertices[vertex["vertex_id"]] = vertex
            else:
                existing["canonical_name"] = min(existing.get("canonical_name", ""),
                                                  vertex.get("canonical_name", ""))
                existing["source_document_ids"] = sorted(set(existing["source_document_ids"])
                                                           | set(vertex["source_document_ids"]))
                existing["source_chunk_ids"] = sorted(set(existing.get("source_chunk_ids", []))
                                                        | set(vertex.get("source_chunk_ids", [])))
                source_hashes = sorted((set(existing.get("source_hashes",
                    [existing.get("source_hash", "")]))
                    | set(vertex.get("source_hashes", [vertex.get("source_hash", "")])))-{""})
                existing["source_hashes"] = source_hashes
                existing["source_hash"] = aggregate_source_hash(
                    source_hashes)
        if edge is not None:
            existing_edge = edges.get(edge["edge_id"])
            if existing_edge is None:
                edges[edge["edge_id"]] = edge
            else:
                existing_edge["source_document_ids"] = sorted(
                    set(existing_edge["source_document_ids"]) | set(edge["source_document_ids"]))
                existing_edge["source_chunk_ids"] = sorted(
                    set(existing_edge["source_chunk_ids"]) | set(edge["source_chunk_ids"]))
                source_hashes = sorted(set(existing_edge.get("source_hashes", []))
                                       | set(edge.get("source_hashes", [])))
                existing_edge["source_hashes"] = source_hashes
                existing_edge["source_hash"] = aggregate_source_hash(
                    source_hashes)
    for value in vertices.values():
        value.pop("source_hashes", None)
    for value in edges.values():
        value.pop("source_hashes", None)
    return ([vertices[key] for key in sorted(vertices)], [edges[key] for key in sorted(edges)])


def write_graph_artifact(directory: Path, graph_version: str,
                         vertices: Iterable[Dict[str, Any]],
                         edges: Iterable[Dict[str, Any]],
                         schema_version: str = GRAPH_SCHEMA_VERSION,
                         extractor_version: str = EXTRACTOR_VERSION) -> Path:
    """Write immutable vertex/edge JSONL and publish a manifest last."""
    if not isinstance(graph_version, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", graph_version):
        raise ValueError("graph_version contains unsafe path characters")
    directory.mkdir(parents=True, exist_ok=True)
    root = directory.resolve()
    target = directory / graph_version
    if target.resolve().parent != root:
        raise ValueError("graph_version must remain within output directory")
    if target.exists():
        raise FileExistsError("graph version already exists: %s" % graph_version)
    temporary_directory = Path(tempfile.mkdtemp(prefix=graph_version + ".", dir=directory))
    staged: List[Tuple[Path, Path]] = []
    try:
        for name, values in (("vertices.jsonl", vertices), ("edges.jsonl", edges)):
            descriptor, temp_name = tempfile.mkstemp(prefix=name + ".", suffix=".tmp", dir=temporary_directory)
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                for value in values:
                    output.write(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                            separators=(",", ":")) + "\n")
                output.flush()
                os.fsync(output.fileno())
            staged.append((temp_path, temporary_directory / name))
        for temporary, final in staged:
            os.replace(temporary, final)
        with (temporary_directory / "vertices.jsonl").open(encoding="utf-8") as vertices_file:
            vertex_count = sum(1 for _ in vertices_file)
        with (temporary_directory / "edges.jsonl").open(encoding="utf-8") as edges_file:
            edge_count = sum(1 for _ in edges_file)
        manifest = {
            "graph_version": graph_version, "schema_version": schema_version,
            "extractor_version": extractor_version,
            "vertex_count": vertex_count,
            "edge_count": edge_count,
        }
        for name in ("vertices.jsonl", "edges.jsonl"):
            digest = hashlib.sha256()
            with (temporary_directory / name).open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
            manifest[name.replace(".jsonl", "_sha256")] = digest.hexdigest()
        manifest_path = temporary_directory / "manifest.json"
        temporary = temporary_directory / "manifest.json.tmp"
        temporary.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, manifest_path)
        os.replace(temporary_directory, target)
        return target / "manifest.json"
    finally:
        for temporary, _ in staged:
            temporary.unlink(missing_ok=True)
        if temporary_directory.exists() and not target.exists():
            for child in temporary_directory.iterdir():
                child.unlink(missing_ok=True)
            temporary_directory.rmdir()
