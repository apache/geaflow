# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

import numpy as np

from lightmem.types import ActionType, MemoryUnit, RecallHit, RecallResult


class _LocalEmbedder:
    def __init__(self, dimension: int = 64) -> None:
        self._dimension = max(8, int(dimension))

    def embed(self, text: str) -> np.ndarray:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        raw = np.frombuffer(digest, dtype=np.uint8).astype(np.float32)
        vec = np.resize(raw, self._dimension)
        norm = np.linalg.norm(vec)
        return vec if norm == 0 else vec / norm


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def _scope_matches(scope_filter: dict[str, str], unit_scope: dict[str, str]) -> bool:
    for k, v in scope_filter.items():
        if unit_scope.get(k) != v:
            return False
    return True


class VectorView:
    """In-memory vector index with scope filtering."""

    def __init__(self, embedder: _LocalEmbedder | None = None) -> None:
        self._embedder = embedder or _LocalEmbedder()
        self._units: dict[str, MemoryUnit] = {}
        self._vectors: dict[str, np.ndarray] = {}

    def upsert(self, unit: MemoryUnit) -> None:
        vector = (
            unit.embedding if unit.embedding is not None else self._embedder.embed(unit.content)
        )
        self._units[unit.id] = replace(unit, embedding=vector)
        self._vectors[unit.id] = vector

    def delete(self, unit_id: str) -> None:
        self._units.pop(unit_id, None)
        self._vectors.pop(unit_id, None)

    def apply_action(self, action_type: ActionType, unit: MemoryUnit | None) -> None:
        if action_type in (ActionType.ADD, ActionType.UPDATE):
            if unit is None:
                return
            self.upsert(unit)
        elif action_type == ActionType.DELETE:
            if unit is None:
                return
            self.delete(unit.id)

    def recall(self, *, query: str, scope_filter: dict[str, str], limit: int = 5) -> RecallResult:
        q = self._embedder.embed(query)
        hits: list[RecallHit] = []
        for unit_id, unit in self._units.items():
            unit_scope: dict[str, Any] = unit.metadata.get("scope") or {}
            unit_scope = {k: str(v) for k, v in unit_scope.items()}
            if not _scope_matches(scope_filter, unit_scope):
                continue
            vec = self._vectors.get(unit_id)
            if vec is None:
                continue
            hits.append(
                RecallHit(
                    unit=unit,
                    similarity=_cosine_similarity(q, vec),
                    view_source="vector",
                )
            )
        hits.sort(key=lambda h: h.similarity, reverse=True)
        return RecallResult(hits=hits[: max(0, int(limit))])


class ViewManager:
    def __init__(self) -> None:
        self.vector_view = VectorView()
