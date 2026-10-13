import hashlib
import json
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
from pathlib import Path

from tools.graphrag.ingest.normalize import NORMALIZATION_VERSION, ChunkingConfig
from tools.graphrag.ingest.pipeline import ArtifactBuildResult, ImportFailure, JsonlRecords, run_import
from tools.graphrag.ingest.sources.source_loader import SourceLoader, SourceManifest


class PipelineTest(unittest.TestCase):

    def test_publishes_ready_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q", "context": [["A", ["A text"]]]}),
                              encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "dev", None, str(source), checksum, "p")

            def build(name):
                return lambda output, chunks: _artifact(output, name, chunks)

            first = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                               build("bm25"), build("vector"))
            second = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            self.assertEqual(first, second)
            self.assertEqual("READY", first["state"])
            self.assertIn("quality_report", first)
            report = json.loads((root / "versions" / first["version"]
                                 / first["quality_report"]).read_text(encoding="utf-8"))
            self.assertEqual(1, report["documents"])
            self.assertEqual(1, report["chunks"])
            self.assertIn("artifact_checksums", first)
            self.assertTrue(first["artifact_checksums"])
            self.assertEqual(NORMALIZATION_VERSION, first["preprocessing_version"])

    def test_corrupt_ready_state_is_not_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            result = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            ready_path = root / "versions" / result["version"] / "READY.json"
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            ready["state"] = "IMPORTING"
            ready_path.write_text(json.dumps(ready), encoding="utf-8")
            with self.assertRaisesRegex(ImportFailure, "not READY"):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           build("bm25"), build("vector"))

    def test_manifest_identity_includes_vector_and_seed_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "dev", None, str(source), checksum, "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            first = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                               build("bm25"), build("vector"), "embed", "v1", 7)
            second = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"), "embed", "v2", 7)
            self.assertNotEqual(first["version"], second["version"])

    def test_conflicting_ready_metadata_is_not_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            result = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            ready_path = root / "versions" / result["version"] / "READY.json"
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            ready["vector_version"] = "tampered"
            ready_path.write_text(json.dumps(ready), encoding="utf-8")
            with self.assertRaises(ImportFailure):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           build("bm25"), build("vector"))

    def test_corrupt_ready_artifact_is_not_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            result = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            artifact = root / "versions" / result["version"] / result["bm25_artifact"]
            artifact.write_text("corrupt", encoding="utf-8")

            with self.assertRaisesRegex(ImportFailure, "checksum mismatch"):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           build("bm25"), build("vector"))

    def test_ready_requires_checksums_for_all_source_and_graph_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            result = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            checksums = result["artifact_checksums"]
            self.assertIn("chunks.jsonl", checksums)
            self.assertIn("documents.jsonl", checksums)
            self.assertIn(result["graph_manifest"], checksums)
            self.assertIn("quality-report.json", checksums)

            ready_path = root / "versions" / result["version"] / "READY.json"
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            del ready["artifact_checksums"]["chunks.jsonl"]
            ready_path.write_text(json.dumps(ready), encoding="utf-8")
            with self.assertRaisesRegex(ImportFailure, "checksum manifest"):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           build("bm25"), build("vector"))

    def test_custom_graph_schema_and_extractor_are_in_manifest_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)

            with patch("tools.graphrag.ingest.pipeline.EXTRACTOR_VERSION", "extractor-a"):
                first = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                   build("bm25"), build("vector"), entity_schema_version="schema-a")
            first_root = root / "versions" / first["version"]
            graph = json.loads((first_root / first["graph_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual("schema-a", graph["schema_version"])
            self.assertEqual("extractor-a", graph["extractor_version"])

            with patch("tools.graphrag.ingest.pipeline.EXTRACTOR_VERSION", "extractor-b"):
                second = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                    build("bm25"), build("vector"), entity_schema_version="schema-a")
            self.assertNotEqual(first["version"], second["version"])

    def test_quality_report_counts_entities_and_edges_without_source_chunks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            record = {"id": "d", "question": "q", "context": [["Alpha", ["Alpha text"]]],
                      "evidences": [["Ghost", "related to", "Unknown"]]}
            source.write_text(json.dumps(record), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            result = run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                build("bm25"), build("vector"))
            final = root / "versions" / result["version"]
            report = json.loads((final / result["quality_report"]).read_text(encoding="utf-8"))
            self.assertEqual(1, report["quarantined_records"])
            self.assertEqual({"vertices": 0, "edges": 0}, report["missing_provenance"])
            self.assertEqual(0, report["entity_chunk_consistency"]["missing_entity_chunks"])
            self.assertEqual(0, report["entity_chunk_consistency"]["missing_edge_chunks"])
            self.assertEqual(1.0, report["index_coverage"]["bm25"]["ratio"])
            self.assertEqual(1.0, report["index_coverage"]["vector"]["ratio"])

    def test_failed_index_is_not_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q", "context": [["A", ["A text"]]]}),
                              encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "dev", None, str(source), checksum, "p")
            with self.assertRaises(ImportFailure):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           lambda output, chunks: (_ for _ in ()).throw(ValueError("bad index")),
                           lambda output, chunks: output / "vector")
            self.assertEqual(0, len(list((root / "versions").glob("*/READY.json"))))
            self.assertEqual([], [path for path in (root / "versions").iterdir() if path.is_dir()])
            self.assertEqual(1, len(list((root / "versions").glob("*.FAILED.json"))))

    def test_index_with_incomplete_chunk_coverage_is_not_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")

            def incomplete(output, chunks):
                path = output / "incomplete.artifact"
                path.write_text("present", encoding="utf-8")
                return ArtifactBuildResult.from_chunk_ids(path, [])

            with self.assertRaisesRegex(ImportFailure, "coverage"):
                run_import(manifest, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                           incomplete, incomplete)
            self.assertEqual([], list((root / "versions").glob("*/READY.json")))

    def test_release_changes_artifact_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text(json.dumps({"id": "d", "question": "q",
                                          "context": [["A", ["A text"]]]}), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            build = lambda name: lambda output, chunks: _artifact(output, name, chunks)
            first = SourceManifest("d", "release-1", "dev", None, str(source), checksum, "p")
            second = SourceManifest("d", "release-2", "dev", None, str(source), checksum, "p")
            first_result = run_import(first, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                      build("bm25"), build("vector"))
            second_result = run_import(second, root / "versions", SourceLoader(), ChunkingConfig(50, 5),
                                       build("bm25"), build("vector"))
            self.assertNotEqual(first_result["version"], second_result["version"])

    def test_large_import_spills_repeatable_chunks_and_cleans_transient_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            with source.open("w", encoding="utf-8") as output:
                for index in range(10000):
                    output.write(json.dumps({"id": str(index), "question": "q",
                                             "context": [["Shared", ["Shared evidence " + str(index)]]]
                                             }) + "\n")
            manifest = SourceManifest("d", "r", "dev", None, str(source),
                                      hashlib.sha256(source.read_bytes()).hexdigest(), "p")
            passes = []

            def build(output, chunks):
                self.assertIsInstance(chunks, JsonlRecords)
                self.assertEqual(10000, len(chunks))
                digest = hashlib.sha256()
                for chunk in chunks:
                    self.assertTrue(chunk["source_uri"].startswith("file://"))
                    digest.update(chunk["chunk_id"].encode("utf-8"))
                passes.append(digest.hexdigest())
                return _artifact(output, "index-" + str(len(passes)), chunks)

            tracemalloc.start()
            try:
                result = run_import(manifest, root / "versions", SourceLoader(batch_size=32),
                                    ChunkingConfig(100, 0), build, build)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(passes[0], passes[1])
            self.assertEqual(10000, result["documents"])
            self.assertEqual(10000, result["chunks"])
            self.assertLess(peak, 12 * 1024 * 1024)
            final = root / "versions" / result["version"]
            self.assertTrue((final / "documents.jsonl").is_file())
            self.assertFalse((final / "graph-staging.sqlite").exists())


def _artifact(output, name, chunks):
    path = output / (name + ".artifact")
    path.write_text(str(len(chunks)), encoding="utf-8")
    return ArtifactBuildResult.from_chunk_ids(path, (chunk["chunk_id"] for chunk in chunks))


if __name__ == "__main__":
    unittest.main()
