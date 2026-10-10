import hashlib
import gzip
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from tools.graphrag.ingest.sources.source_loader import SourceError, SourceLoader, SourceManifest


class SourceLoaderTest(unittest.TestCase):

    def test_loads_jsonl_in_stable_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            records = [
                {"id": "a", "question": "q1", "context": [["T", ["text"]]], "answer": "a1",
                 "evidences": [["T", "rel", "U"]]},
                {"id": "b", "question": "q2", "context": [{"title": "U", "text": "text"}], "answer": ["a2"]},
            ]
            source.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
            batches = list(SourceLoader(batch_size=1).load_batches(manifest))
            self.assertEqual([["a"], ["b"]], [[item["document_id"] for item in batch] for batch in batches])
            self.assertEqual([["T", "rel", "U"]], batches[0][0]["evidences"])

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

    def test_streams_json_array(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.json"
            records = [{"id": str(index), "question": "q", "context": [["T", ["x"]]]}
                       for index in range(1200)]
            source.write_text(json.dumps(records), encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
            batches = list(SourceLoader(batch_size=100).load_batches(manifest))
            self.assertEqual([100] * 12, [len(batch) for batch in batches])

    def test_rejects_trailing_comma_and_trailing_data(self):
        for contents in ("[{} ,]", "[{}]garbage"):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "records.json"
                source.write_text(contents, encoding="utf-8")
                checksum = hashlib.sha256(source.read_bytes()).hexdigest()
                manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
                with self.assertRaises(SourceError):
                    list(SourceLoader().load(manifest))

    def test_limits_uncompressed_plain_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl"
            source.write_text(json.dumps({"id": "a", "question": "q", "context": []}),
                              encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
            with self.assertRaisesRegex(SourceError, "uncompressed size limit"):
                list(SourceLoader(max_uncompressed_bytes=1).load(manifest))

    def test_limits_uncompressed_gzip_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "records.jsonl.gz"
            contents = json.dumps({"id": "a", "question": "q", "context": []})
            with gzip.open(source, "wt", encoding="utf-8") as stream:
                stream.write(contents)
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("d", "r", "s", None, str(source), checksum, "p")
            with self.assertRaisesRegex(SourceError, "gzip source exceeds"):
                list(SourceLoader(max_uncompressed_bytes=len(contents) - 1).load(manifest))

    def test_selects_manifest_split_from_multi_file_zip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "data.zip"
            payload = lambda value: json.dumps({"id": value, "question": "q",
                                                 "context": [["T", ["text"]]]})
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr("train.json", payload("train"))
                archive.writestr("dev.json", payload("dev"))
                archive.writestr("test.json", payload("test"))
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = SourceManifest("2wikimultihopqa", "r", "dev", None, str(source), checksum, "p")
            records = list(SourceLoader().load(manifest))
            self.assertEqual(["dev"], [record["document_id"] for record in records])

    def test_zip_rejects_missing_or_ambiguous_split(self):
        for entries in (("train.json", "test.json"), ("dev.json", "folder/DEV.JSON")):
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "data.zip"
                with zipfile.ZipFile(source, "w") as archive:
                    for name in entries:
                        archive.writestr(name, "[]")
                manifest = SourceManifest("2wikimultihopqa", "r", "dev", None, str(source),
                                          hashlib.sha256(source.read_bytes()).hexdigest(), "p")
                with self.assertRaisesRegex(SourceError, "matching data files"):
                    list(SourceLoader().load(manifest))


if __name__ == "__main__":
    unittest.main()
