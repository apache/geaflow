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

from dataclasses import dataclass
import hashlib
import threading
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from api.envelope import Envelope
from api.schema import RequestGraphSchema
from core.config import DefaultConfiguration
from core.gremlin_state import GremlinStateMachine
from core.interfaces import Configuration, EmbeddingServiceProtocol
from core.models import Context, StrategyKnowledgeUnit
from core.strategy_cache import StrategyCache
from services.embedding import EmbeddingService
from services.llm_oracle import LLMOracle


class _RequestScopedConfig(Configuration):
    def __init__(self, base: Configuration, overrides: dict[str, Any]):
        self._base = base
        self._overrides = dict(overrides)

    def get(self, key: str, default: Any) -> Any:
        if key in self._overrides:
            return self._overrides[key]
        return self._base.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        return int(self.get(key, default))

    def get_float(self, key: str, default: float = 0.0) -> float:
        return float(self.get(key, default))

    def get_bool(self, key: str, default: bool = False) -> bool:
        return bool(self.get(key, default))

    def get_str(self, key: str, default: str = "") -> str:
        return str(self.get(key, default))

    def get_llm_config(self) -> dict[str, str]:
        return self._base.get_llm_config()

    def get_embedding_config(self) -> dict[str, str]:
        return self._base.get_embedding_config()


class _LocalEmbeddingService:
    def __init__(self, dimension: int = 64) -> None:
        self._dimension = max(8, int(dimension))

    async def embed_text(self, text: str) -> np.ndarray:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        raw = np.frombuffer(digest, dtype=np.uint8).astype(np.float32)
        tiled = np.resize(raw, self._dimension)
        norm = np.linalg.norm(tiled)
        return tiled if norm == 0 else tiled / norm

    async def embed_properties(self, properties: dict[str, Any]) -> np.ndarray:
        parts = [f"{k}={properties[k]}" for k in sorted(properties.keys())]
        return await self.embed_text("|".join(parts))


class _TraversalPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    structural_signature: str
    step_index: int | None = None


class _NodePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    label: str
    properties: dict[str, Any] = Field(default_factory=dict)


class _GraphSchemaPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_fingerprint: str
    valid_outgoing_labels: list[str] = Field(default_factory=list)
    valid_incoming_labels: list[str] = Field(default_factory=list)
    schema_summary: str | None = None
    node_types: list[str] | None = None
    edge_labels: list[str] | None = None
    node_schema: dict[str, Any] | None = None


class CastsDecisionPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    goal: str
    max_depth: int | None = None
    traversal: _TraversalPayload
    node: _NodePayload
    graph_schema: _GraphSchemaPayload
    constraints: dict[str, Any] | None = None


@dataclass
class _CastsRuntime:
    cache_namespace: str
    schema_fingerprint: str
    cache: StrategyCache
    llm_oracle: LLMOracle | None


class _CastsRuntimeManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runtimes: dict[str, _CastsRuntime] = {}

    def get_runtime(self, *, run_id: str, schema_fingerprint: str) -> _CastsRuntime:
        key = f"{run_id}|{schema_fingerprint}"
        with self._lock:
            runtime = self._runtimes.get(key)
            if runtime is not None:
                return runtime

            base_config = DefaultConfiguration()
            config = _RequestScopedConfig(
                base_config,
                overrides={
                    "CACHE_SCHEMA_FINGERPRINT": schema_fingerprint,
                    # Never eval arbitrary code from LLM predicates by default.
                    "LLM_ORACLE_ENABLE_PREDICATE_EVAL": False,
                },
            )

            try:
                embed: EmbeddingServiceProtocol = EmbeddingService(config)
            except Exception:
                embed = _LocalEmbeddingService()

            cache = StrategyCache(embed, config)

            try:
                oracle = LLMOracle(embed, config)
            except Exception:
                oracle = None

            runtime = _CastsRuntime(
                cache_namespace=run_id,
                schema_fingerprint=schema_fingerprint,
                cache=cache,
                llm_oracle=oracle,
            )
            self._runtimes[key] = runtime
            return runtime


_RUNTIME_MANAGER = _CastsRuntimeManager()


def _schema_from_payload(p: _GraphSchemaPayload) -> RequestGraphSchema:
    return RequestGraphSchema(
        schema_fingerprint=p.schema_fingerprint,
        valid_outgoing_labels=p.valid_outgoing_labels,
        valid_incoming_labels=p.valid_incoming_labels,
        node_types=set(p.node_types or []),
        edge_labels=set(p.edge_labels or []),
        node_schema=p.node_schema or {},
    )


def _ensure_node_type(properties: dict[str, Any], fallback_label: str) -> dict[str, Any]:
    if "type" not in properties and fallback_label:
        return {**properties, "type": fallback_label}
    return properties


def _dedupe_existing(cache: StrategyCache, new_sku: StrategyKnowledgeUnit) -> StrategyKnowledgeUnit:
    for existing in cache.knowledge_base:
        if (
            existing.structural_signature == new_sku.structural_signature
            and existing.goal_template == new_sku.goal_template
            and existing.decision_template == new_sku.decision_template
        ):
            return existing
    cache.add_sku(new_sku)
    return new_sku


async def decide(envelope: Envelope) -> dict[str, Any]:
    """Core CASTS decision flow for one traversal step."""

    try:
        payload = CastsDecisionPayload.model_validate(envelope.payload)
    except Exception as e:
        return {
            "decision": "stop",
            "match_type": "STOP_INVALID",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": None,
                "cache_namespace": envelope.scope.run_id,
                "error": f"invalid_payload: {type(e).__name__}",
            },
        }

    run_id = (envelope.scope.run_id or "").strip()
    if not run_id:
        return {
            "decision": "stop",
            "match_type": "STOP_INVALID",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": payload.graph_schema.schema_fingerprint,
                "cache_namespace": None,
                "error": "scope.run_id is required for CASTS cache isolation",
            },
        }

    runtime = _RUNTIME_MANAGER.get_runtime(
        run_id=run_id, schema_fingerprint=payload.graph_schema.schema_fingerprint
    )

    schema = _schema_from_payload(payload.graph_schema)
    node_properties = _ensure_node_type(payload.node.properties, payload.node.label)
    context = Context(
        structural_signature=payload.traversal.structural_signature,
        properties=node_properties,
        goal=payload.goal,
    )

    try:
        decision, sku, match_type = await runtime.cache.find_strategy(context)
    except Exception as e:
        return {
            "decision": "stop",
            "match_type": "EMBEDDING_ERROR",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
                "error": f"embedding_error: {type(e).__name__}",
            },
        }
    if match_type in ("Tier1", "Tier2") and decision:
        return {
            "decision": decision,
            "match_type": match_type,
            "sku_id": getattr(sku, "id", None) if sku else None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
            },
        }

    if runtime.llm_oracle is None:
        return {
            "decision": "stop",
            "match_type": "LLM_DISABLED",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
            },
        }

    state, next_step_options = GremlinStateMachine.get_state_and_options(
        context.structural_signature, schema, str(context.properties.get("type") or "")
    )
    if state == "END" or not next_step_options:
        return {
            "decision": "stop",
            "match_type": "STOP_END",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
            },
        }

    try:
        new_sku = await runtime.llm_oracle.generate_sku(context, schema)
    except Exception as e:
        return {
            "decision": "stop",
            "match_type": "LLM_ERROR",
            "sku_id": None,
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
                "error": f"llm_oracle_error: {type(e).__name__}",
            },
        }
    new_sku = _dedupe_existing(runtime.cache, new_sku)

    # Safety: Project decision into allowed options.
    if new_sku.decision_template not in next_step_options:
        return {
            "decision": "stop",
            "match_type": "STOP_INVALID",
            "sku_id": getattr(new_sku, "id", None),
            "provenance": {
                "trace_id": envelope.trace.trace_id,
                "schema_fingerprint": runtime.schema_fingerprint,
                "cache_namespace": runtime.cache_namespace,
                "error": "decision not in next_step_options",
            },
        }

    return {
        "decision": new_sku.decision_template,
        "match_type": "LLM",
        "sku_id": getattr(new_sku, "id", None),
        "provenance": {
            "trace_id": envelope.trace.trace_id,
            "schema_fingerprint": runtime.schema_fingerprint,
            "cache_namespace": runtime.cache_namespace,
        },
    }
