import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tools.graphrag.ingest.sources.source_loader import SourceError, SourceLoader, SourceManifest


class SourceLoaderTest(unittest.TestCase):

    def test_loads_jsonl_in_stable_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            records = [
                {"id": "a", "question": "q1", "context": [["T", ["text"]]], "answer": "a1"},
                {"id": "b", "question": "q2", "context": [{"title": "U", "text": "text"}], "answer": ["a2"]},
            ]
            source.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
            batches = list(SourceLoader(batch_size=1).load_batches(manifest))
            self.assertEqual([["a"], ["b"]], [[item["document_id"] for item in batch] for batch in batches])

    def test_checksum_mismatch_fails_before_parsing(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.json"
            source.write_text("not json", encoding="utf-8")
            manifest = SourceManifest("d", "r", "s", None, str(source), "0" * 64, "p")
            with self.assertRaisesRegex(SourceError, "checksum mismatch"):
                list(SourceLoader().load(manifest))

    def test_malformed_record_reports_location(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            source.write_text(json.dumps({"id": "a"}), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "dev", None, str(source), checksum, "p")
            with self.assertRaisesRegex(SourceError, "d/dev record 1 field question"):
                list(SourceLoader().load(manifest))


if __name__ == "__main__":
    unittest.main()
