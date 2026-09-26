import unittest

from tools.graphrag.ingest.normalize import ChunkingConfig, chunk_document, documents, normalize_text


class NormalizeTest(unittest.TestCase):

    def test_normalizes_unicode_and_whitespace(self):
        self.assertEqual("Cafe\u0301".replace("e\u0301", "é"), normalize_text("Cafe\u0301  \r\n \t"))

    def test_chunks_have_stable_ids_offsets_and_overlap(self):
        record = {
            "dataset": "d", "dataset_release": "r", "split": "dev", "document_id": "x",
            "question": " q  ? ", "paragraphs": [{"title": "T", "text": "alpha beta gamma delta"}],
        }
        document = list(documents([record]))[0]
        config = ChunkingConfig(size=12, overlap=3, token_characters=4)
        first = chunk_document(document, config)
        second = chunk_document(document, config)
        self.assertEqual(first, second)
        self.assertGreater(len(first), 1)
        self.assertLessEqual(first[1]["start_offset"], first[0]["end_offset"])
        for chunk in first:
            self.assertEqual(document["document_id"], chunk["document_id"])
            self.assertEqual(chunk["text"], document["text"][chunk["start_offset"]:chunk["end_offset"]])

    def test_offsets_use_utf16_units_for_non_bmp_text(self):
        record = {
            "dataset": "d", "dataset_release": "r", "split": "dev", "document_id": "x",
            "question": "q", "paragraphs": [{"title": "T", "text": "a😀b"}],
        }
        document = list(documents([record]))[0]
        chunk = chunk_document(document, ChunkingConfig(size=20, overlap=0))[0]
        self.assertEqual(0, chunk["start_offset"])
        self.assertEqual(6, chunk["end_offset"])

    def test_rejects_invalid_overlap(self):
        with self.assertRaises(ValueError):
            ChunkingConfig(size=10, overlap=10)


if __name__ == "__main__":
    unittest.main()
