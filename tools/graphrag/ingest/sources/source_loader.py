"""Manifest-driven readers for the Week 2 public datasets.

The reader intentionally depends only on the Python standard library.  It accepts JSON arrays,
JSONL files, and gzip-compressed variants, while yielding canonical records in bounded batches.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional


class SourceError(ValueError):
    """Raised when a source cannot be verified or parsed into canonical records."""


@dataclass(frozen=True)
class SourceManifest:
    dataset: str
    dataset_release: str
    split: str
    source_uri: Optional[str]
    cache_path: Optional[str]
    sha256: str
    parser_version: str

    @classmethod
    def from_file(cls, path: Path) -> "SourceManifest":
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SourceError("cannot read manifest %s: %s" % (path, exc)) from exc
        if not isinstance(raw, dict):
            raise SourceError("manifest %s must contain a JSON object" % path)
        raw = {key: os.path.expandvars(value) if isinstance(value, str) else value
               for key, value in raw.items()}
        required = ("dataset", "dataset_release", "split", "sha256", "parser_version")
        missing = [key for key in required if not str(raw.get(key, "")).strip()]
        if missing:
            raise SourceError("manifest %s is missing: %s" % (path, ", ".join(missing)))
        if not raw.get("source_uri") and not raw.get("cache_path"):
            raise SourceError("manifest %s requires source_uri or cache_path" % path)
        checksum = str(raw["sha256"]).lower()
        if len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum):
            raise SourceError("manifest %s has invalid sha256" % path)
        if raw.get("dataset") == "2wikimultihopqa" and raw.get("split") not in ("dev", "test"):
            raise SourceError("2wikimultihopqa manifest split must be dev or test")
        return cls(
            dataset=str(raw["dataset"]),
            dataset_release=str(raw["dataset_release"]),
            split=str(raw["split"]),
            source_uri=raw.get("source_uri"),
            cache_path=raw.get("cache_path"),
            sha256=checksum,
            parser_version=str(raw["parser_version"]),
        )


class SourceLoader:
    """Loads and validates one manifest without exposing dataset-specific records."""

    def __init__(self, allow_download: bool = False, batch_size: int = 256,
                 max_download_bytes: int = 1024 * 1024 * 1024,
                 max_uncompressed_bytes: int = 1024 * 1024 * 1024,
                 max_zip_entries: int = 16, timeout: float = 30.0):
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if min(max_download_bytes, max_uncompressed_bytes, max_zip_entries) < 1 or timeout <= 0:
            raise ValueError("source limits must be positive")
        self.allow_download = allow_download
        self.batch_size = batch_size
        self.max_download_bytes = max_download_bytes
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_zip_entries = max_zip_entries
        self.timeout = timeout

    def load_batches(self, manifest: SourceManifest) -> Iterator[List[Dict[str, Any]]]:
        path = self._resolve_source(manifest)
        self._verify_checksum(path, manifest)
        batch: List[Dict[str, Any]] = []
        if path.stat().st_size > self.max_download_bytes:
            raise SourceError("source exceeds configured size limit: %s" % path)
        for number, raw in enumerate(self._records(path, self.max_uncompressed_bytes,
                                                   self.max_zip_entries), start=1):
            batch.append(self._canonical_record(manifest, number, raw))
            if len(batch) == self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def load(self, manifest: SourceManifest) -> Iterator[Dict[str, Any]]:
        for batch in self.load_batches(manifest):
            yield from batch

    def _resolve_source(self, manifest: SourceManifest) -> Path:
        if manifest.cache_path:
            path = Path(os.path.expandvars(manifest.cache_path)).expanduser()
            if path.is_file():
                return path
        if not self.allow_download or not manifest.source_uri:
            location = manifest.cache_path or "<no cache path>"
            raise SourceError(
                "%s/%s cache is missing at %s; enable download explicitly or provide a verified cache"
                % (manifest.dataset, manifest.split, location)
            )
        target = Path(os.path.expandvars(manifest.cache_path or Path(manifest.source_uri).name)).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".part",
                                                       dir=str(target.parent))
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            request = urllib.request.Request(manifest.source_uri)
            total = 0
            with urllib.request.urlopen(request, timeout=self.timeout) as response, temporary.open("wb") as output:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    total += len(block)
                    if total > self.max_download_bytes:
                        raise SourceError("download exceeds configured size limit")
                    output.write(block)
            self._verify_checksum(temporary, manifest)
            temporary.replace(target)
        except (OSError, SourceError) as exc:
            try:
                temporary.unlink()
            except OSError:
                pass
            if isinstance(exc, SourceError):
                raise
            raise SourceError("download failed for %s/%s: %s" % (manifest.dataset, manifest.split, exc)) from exc
        return target

    @staticmethod
    def _verify_checksum(path: Path, manifest: SourceManifest) -> None:
        digest = hashlib.sha256()
        try:
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
        except OSError as exc:
            raise SourceError("cannot read %s/%s source %s: %s" % (
                manifest.dataset, manifest.split, path, exc)) from exc
        actual = digest.hexdigest()
        if actual != manifest.sha256:
            raise SourceError(
                "%s/%s checksum mismatch for %s: expected %s, got %s"
                % (manifest.dataset, manifest.split, path, manifest.sha256, actual)
            )

    @staticmethod
    def _records(path: Path, max_uncompressed_bytes: int = 1024 * 1024 * 1024,
                 max_zip_entries: int = 16) -> Iterable[Any]:
        if path.suffix == ".zip":
            try:
                with zipfile.ZipFile(path) as archive:
                    names = sorted(name for name in archive.namelist()
                                   if name.endswith((".json", ".jsonl", ".json.gz", ".jsonl.gz")))
                    if len(archive.namelist()) > max_zip_entries:
                        raise SourceError("ZIP source %s has too many entries" % path)
                    if len(names) != 1:
                        raise SourceError("ZIP source %s must contain exactly one JSON data file" % path)
                    descriptor, temporary_name = tempfile.mkstemp(suffix=Path(names[0]).suffix)
                    os.close(descriptor)
                    extracted = Path(temporary_name)
                    try:
                        total = 0
                        with archive.open(names[0]) as source, extracted.open("wb") as target:
                            while True:
                                block = source.read(1024 * 1024)
                                if not block:
                                    break
                                total += len(block)
                                if total > max_uncompressed_bytes:
                                    raise SourceError("ZIP entry exceeds configured size limit")
                                target.write(block)
                        yield from SourceLoader._records(extracted, max_uncompressed_bytes, max_zip_entries)
                    finally:
                        extracted.unlink(missing_ok=True)
                return
            except (OSError, zipfile.BadZipFile) as exc:
                raise SourceError("cannot parse ZIP source %s: %s" % (path, exc)) from exc
        if path.suffix == ".gz":
            descriptor, temporary_name = tempfile.mkstemp(suffix=".jsonl")
            os.close(descriptor)
            extracted = Path(temporary_name)
            try:
                total = 0
                with gzip.open(path, "rb") as source, extracted.open("wb") as target:
                    while True:
                        block = source.read(1024 * 1024)
                        if not block:
                            break
                        total += len(block)
                        if total > max_uncompressed_bytes:
                            raise SourceError("gzip source exceeds configured size limit")
                        target.write(block)
                yield from SourceLoader._records(extracted, max_uncompressed_bytes, max_zip_entries)
            except SourceError:
                raise
            except (OSError, gzip.BadGzipFile) as exc:
                raise SourceError("cannot parse gzip source %s: %s" % (path, exc)) from exc
            finally:
                extracted.unlink(missing_ok=True)
            return
        try:
            if path.stat().st_size > max_uncompressed_bytes:
                raise SourceError("source exceeds configured uncompressed size limit: %s" % path)
        except OSError as exc:
            raise SourceError("cannot stat source %s: %s" % (path, exc)) from exc
        opener = open
        try:
            with opener(path, "rt", encoding="utf-8") as stream:
                first = stream.read(1)
                stream.seek(0)
                if first == "[":
                    yield from SourceLoader._array_records(stream, path)
                else:
                    for line_number, line in enumerate(stream, start=1):
                        if line.strip():
                            try:
                                yield json.loads(line)
                            except json.JSONDecodeError as exc:
                                raise SourceError("invalid JSON at %s line %d: %s" % (
                                    path, line_number, exc)) from exc
        except SourceError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise SourceError("cannot parse source %s: %s" % (path, exc)) from exc

    @staticmethod
    def _array_records(stream: Any, path: Path) -> Iterator[Any]:
        decoder = json.JSONDecoder()
        buffer = ""
        opened = False
        expect_value = True
        after_comma = False
        closed = False
        while True:
            chunk = stream.read(64 * 1024)
            if chunk:
                buffer += chunk
            end = not chunk
            while True:
                buffer = buffer.lstrip()
                if closed:
                    if buffer:
                        raise SourceError("trailing data after JSON array in %s" % path)
                    break
                if not opened:
                    if not buffer:
                        break
                    if buffer[0] != "[":
                        raise SourceError("JSON source %s must contain an array" % path)
                    buffer = buffer[1:]
                    opened = True
                buffer = buffer.lstrip()
                if not buffer:
                    break
                if expect_value and buffer[0] == "]" and not after_comma:
                    buffer = buffer[1:]
                    closed = True
                    continue
                if not expect_value:
                    if buffer[0] != ",":
                        if buffer[0] == "]":
                            buffer = buffer[1:]
                            closed = True
                            continue
                        raise SourceError("invalid JSON array separator in %s" % path)
                    buffer = buffer[1:]
                    expect_value = True
                    after_comma = True
                    continue
                try:
                    value, consumed = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    if end:
                        raise SourceError("truncated JSON array in %s" % path)
                    break
                yield value
                buffer = buffer[consumed:]
                expect_value = False
                after_comma = False
            if end:
                if closed:
                    return
                raise SourceError("unterminated JSON array in %s" % path)

    @staticmethod
    def _canonical_record(manifest: SourceManifest, number: int, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise SourceError("%s/%s record %d must be an object" % (
                manifest.dataset, manifest.split, number))
        document_id = raw.get("_id") or raw.get("id") or raw.get("question_id")
        question = raw.get("question")
        if not isinstance(document_id, (str, int)) or not str(document_id).strip():
            raise SourceError("%s/%s record %d field id is required" % (
                manifest.dataset, manifest.split, number))
        if not isinstance(question, str) or not question.strip():
            raise SourceError("%s/%s record %d field question is required" % (
                manifest.dataset, manifest.split, number))
        context = raw.get("context") or raw.get("passages")
        if not isinstance(context, list) or not context:
            raise SourceError("%s/%s record %d field context must be a non-empty list" % (
                manifest.dataset, manifest.split, number))
        paragraphs = []
        for index, item in enumerate(context):
            if isinstance(item, list) and len(item) == 2 and isinstance(item[1], list):
                title, sentences = item
                text = " ".join(str(sentence) for sentence in sentences)
            elif isinstance(item, dict):
                title = item.get("title") or item.get("name") or ""
                text = item.get("text") or item.get("paragraph") or ""
                if isinstance(text, list):
                    text = " ".join(str(sentence) for sentence in text)
            else:
                raise SourceError("%s/%s record %d field context[%d] has unsupported shape" % (
                    manifest.dataset, manifest.split, number, index))
            if not isinstance(title, str) or not isinstance(text, str) or not text.strip():
                raise SourceError("%s/%s record %d field context[%d] is empty" % (
                    manifest.dataset, manifest.split, number, index))
            paragraphs.append({"title": title.strip(), "text": text})
        answers = raw.get("answer") or raw.get("answers")
        if isinstance(answers, str):
            answers = [answers]
        if answers is None:
            answers = []
        if not isinstance(answers, list) or any(not isinstance(answer, str) for answer in answers):
            raise SourceError("%s/%s record %d field answer has invalid shape" % (
                manifest.dataset, manifest.split, number))
        canonical = {
            "dataset": manifest.dataset,
            "dataset_release": manifest.dataset_release,
            "split": manifest.split,
            "document_id": str(document_id),
            "question": question,
            "answers": answers,
            "paragraphs": paragraphs,
            "supporting_facts": raw.get("supporting_facts") or raw.get("evidence") or [],
            "evidences": raw.get("evidences") or [],
        }
        canonical["source_hash"] = hashlib.sha256(
            json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return canonical
