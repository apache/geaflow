import tempfile
import unittest
from pathlib import Path

from tools.graphrag.ingest.digests import sha256_file


class DigestTest(unittest.TestCase):

    def test_sha256_file_reads_binary_content(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.bin"
            path.write_bytes(b"abc")

            self.assertEqual(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                sha256_file(path),
            )

    def test_sha256_file_preserves_file_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.bin"
            with self.assertRaises(FileNotFoundError):
                sha256_file(path)


if __name__ == "__main__":
    unittest.main()
