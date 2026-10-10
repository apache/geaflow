import hashlib
import json
import tempfile
import tracemalloc
import unittest
from pathlib import Path

from tools.graphrag.ingest.normalize import ChunkingConfig
from tools.graphrag.ingest.pipeline import ImportFailure, JsonlRecords, run_import
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
    return path


if __name__ == "__main__":
    unittest.main()
