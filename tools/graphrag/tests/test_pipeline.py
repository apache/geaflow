import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tools.graphrag.ingest.normalize import ChunkingConfig
from tools.graphrag.ingest.pipeline import ImportFailure, run_import
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


def _artifact(output, name, chunks):
    path = output / (name + ".artifact")
    path.write_text(str(len(chunks)), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
