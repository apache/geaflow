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

            build = _artifact_builder

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
            build = _artifact_builder
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
            build = _artifact_builder
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
            build = _artifact_builder
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
            build = _artifact_builder
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
            build = _artifact_builder
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
            build = _artifact_builder

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
            build = _artifact_builder
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
            build = _artifact_builder
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


def _artifact_builder(name):
    def build(output, chunks):
        return _artifact(output, name, chunks)

    return build


if __name__ == "__main__":
    unittest.main()


class JavaManifestTest(unittest.TestCase):
    """The Java manifest must carry every build-metadata field the design requires."""

    def test_carries_entity_extractor_and_budget_configuration(self):
        from tools.graphrag.publish_bundle import _java_manifest

        ready = {
            "dataset": "d", "version": "v1", "dataset_release": "r", "split": "dev",
            "sha256": "0" * 64, "policy_version": "chunk-v1",
            "preprocessing_version": "p", "entity_schema_version": "schema-v2",
            "entity_extractor_version": "extractor-v3",
            "vector_source": "v", "vector_version": "v1", "random_seed": 0,
            "source_uri": "uri",
        }
        config = {"chunk_size": 100, "overlap": 10,
                  "budget_configuration": {"primary_constraint": "tokens",
                                           "max_candidates": 50, "timeout_ms": 1500,
                                           "token_budget": 2048, "max_edge_scans": 60}}

        manifest = _java_manifest(ready, config)

        # Distinct facts: the extractor decides entities, the schema decides their storage.
        self.assertEqual("extractor-v3", manifest["entityExtractorVersion"])
        self.assertEqual("schema-v2", manifest["graphSchemaVersion"])
        self.assertEqual({
            "primaryConstraint": "tokens", "maxCandidates": 50, "timeoutMs": 1500,
            "tokenBudget": 2048, "maxEdgeScans": 60}, manifest["budgetConfiguration"])

    def test_absent_budget_configuration_stays_absent(self):
        from tools.graphrag.publish_bundle import _java_manifest

        ready = {"dataset": "d", "version": "v1", "dataset_release": "r", "split": "dev",
                 "sha256": "0" * 64, "policy_version": "p", "preprocessing_version": "p",
                 "entity_schema_version": "s", "entity_extractor_version": "e",
                 "vector_source": "v", "vector_version": "v1", "random_seed": 0}

        manifest = _java_manifest(ready, {})

        self.assertIsNone(manifest["budgetConfiguration"],
                          "never recorded must not become a silent default")

    def test_every_shipped_manifest_declares_a_budget_configuration(self):
        for path in sorted(Path("tools/graphrag/manifests").glob("*.json")):
            config = json.loads(path.read_text(encoding="utf-8"))
            budget = config.get("budget_configuration")
            self.assertIsNotNone(budget, "%s must declare budget_configuration" % path)
            for key in ("primary_constraint", "max_candidates", "timeout_ms",
                        "token_budget", "max_edge_scans"):
                self.assertIn(key, budget, "%s is missing %s" % (path, key))
            self.assertIn(budget["primary_constraint"],
                          ("candidates", "latency", "tokens"))
            self.assertIsNotNone(config.get("entity_extractor_version"),
                                 "%s must record its entity extractor" % path)
