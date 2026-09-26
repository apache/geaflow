"""Manifest-driven readers for the Week 2 public datasets.

The reader intentionally depends only on the Python standard library.  It accepts JSON arrays,
JSONL files, and gzip-compressed variants, while yielding canonical records in bounded batches.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import urllib.request
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
        required = ("dataset", "dataset_release", "split", "sha256", "parser_version")
        missing = [key for key in required if not str(raw.get(key, "")).strip()]
        if missing:
            raise SourceError("manifest %s is missing: %s" % (path, ", ".join(missing)))
        if not raw.get("source_uri") and not raw.get("cache_path"):
            raise SourceError("manifest %s requires source_uri or cache_path" % path)
        checksum = str(raw["sha256"]).lower()
        if len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum):
            raise SourceError("manifest %s has invalid sha256" % path)
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

    def __init__(self, allow_download: bool = False, batch_size: int = 256):
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.allow_download = allow_download
        self.batch_size = batch_size

    def load_batches(self, manifest: SourceManifest) -> Iterator[List[Dict[str, Any]]]:
        path = self._resolve_source(manifest)
        self._verify_checksum(path, manifest)
        batch: List[Dict[str, Any]] = []
        for number, raw in enumerate(self._records(path), start=1):
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
        try:
            urllib.request.urlretrieve(manifest.source_uri, target)
        except OSError as exc:
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
    def _records(path: Path) -> Iterable[Any]:
        opener = gzip.open if path.suffix == ".gz" else open
        try:
            with opener(path, "rt", encoding="utf-8") as stream:
                first = stream.read(1)
                stream.seek(0)
                if first == "[":
                    values = json.load(stream)
                    if not isinstance(values, list):
                        raise SourceError("JSON source %s must contain an array" % path)
                    yield from values
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
        return {
            "dataset": manifest.dataset,
            "dataset_release": manifest.dataset_release,
            "split": manifest.split,
            "record_number": number,
            "document_id": str(document_id),
            "question": question,
            "answers": answers,
            "paragraphs": paragraphs,
            "supporting_facts": raw.get("supporting_facts") or raw.get("evidence") or [],
        }
