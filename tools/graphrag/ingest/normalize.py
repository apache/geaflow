"""Canonical document normalization and deterministic character chunking."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List


NORMALIZATION_VERSION = "unicode-nfc-whitespace-v1"
CHUNKING_VERSION = "character-window-v1"


@dataclass(frozen=True)
class ChunkingConfig:
    size: int = 1200
    overlap: int = 120
    token_characters: int = 4

    def __post_init__(self) -> None:
        if self.size < 1 or self.overlap < 0 or self.overlap >= self.size:
            raise ValueError("chunk size must be positive and overlap must be in [0, size)")
        if self.token_characters < 1:
            raise ValueError("token_characters must be positive")


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFC", value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    return "\n".join(line for line in lines if line)


def _utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def documents(records: Iterable[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for record in records:
        paragraphs = record["paragraphs"]
        title = normalize_text(" ".join(item["title"] for item in paragraphs if item["title"]))
        body = "\n\n".join(normalize_text(item["text"]) for item in paragraphs)
        text = normalize_text((title + "\n\n" + body) if title else body)
        if not text:
            raise ValueError("record %s normalizes to an empty document" % record["document_id"])
        digest = record.get("source_hash")
        if not digest:
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        yield {
            "document_id": record["document_id"],
            "dataset": record["dataset"],
            "dataset_release": record["dataset_release"],
            "split": record["split"],
            "title": title,
            "source_hash": digest,
            "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "text": text,
            "question": normalize_text(record["question"]),
            "answers": record.get("answers", []),
            "supporting_facts": record.get("supporting_facts", []),
            "normalization_version": NORMALIZATION_VERSION,
        }


def chunk_document(document: Dict[str, Any], config: ChunkingConfig) -> List[Dict[str, Any]]:
    text = document["text"]
    result: List[Dict[str, Any]] = []
    start = 0
    index = 0
    while start < len(text):
        end = min(start + config.size, len(text))
        if end < len(text):
            boundary = max(text.rfind("\n", start + config.size // 2, end),
                           text.rfind(" ", start + config.size // 2, end))
            if boundary > start:
                end = boundary
        chunk_text = text[start:end].strip()
        leading = len(text[start:end]) - len(text[start:end].lstrip())
        actual_start = start + leading
        actual_end = actual_start + len(chunk_text)
        if chunk_text:
            digest = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
            chunk_id = hashlib.sha256((document["document_id"] + "\0" + str(index) + "\0" + digest)
                                      .encode("utf-8")).hexdigest()
            result.append({
                "chunk_id": chunk_id,
                "document_id": document["document_id"],
                "chunk_index": index,
                "start_offset": _utf16_length(text[:actual_start]),
                "end_offset": _utf16_length(text[:actual_end]),
                "token_estimate": (len(chunk_text) + config.token_characters - 1)
                    // config.token_characters,
                "text": chunk_text,
                "policy_version": CHUNKING_VERSION,
                "text_hash": digest,
                "source_hash": document["source_hash"],
            })
            index += 1
        if end == len(text):
            break
        start = max(end - config.overlap, start + 1)
    return result


def chunks(documents_iter: Iterable[Dict[str, Any]], config: ChunkingConfig
           ) -> Iterator[Dict[str, Any]]:
    for document in documents_iter:
        yield from chunk_document(document, config)
