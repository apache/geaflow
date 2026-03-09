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

import uuid
from dataclasses import asdict
from typing import Any

from lightmem.ledger import Ledger
from lightmem.types import Action, ActionType, MemoryUnit, RecallResult
from lightmem.views import ViewManager


def _normalize_scope(scope: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in scope.items() if v is not None and str(v).strip()}


class MemoryKernel:
    def __init__(self) -> None:
        self.ledger = Ledger()
        self.views = ViewManager()

    def write(
        self,
        *,
        messages: list[dict[str, Any]],
        scope: dict[str, Any],
        mode: str = "echo",
    ) -> dict[str, Any]:
        scope_norm = _normalize_scope(scope)
        if not scope_norm:
            raise ValueError("scope is required")

        actions: list[Action] = []

        if mode not in ("echo", "llm"):
            mode = "echo"

        if mode == "echo":
            for msg in messages:
                if msg.get("role") != "user":
                    continue
                content = str(msg.get("content") or "").strip()
                if not content:
                    continue
                unit_id = f"mem_{uuid.uuid4().hex}"
                actions.append(Action(event_type=ActionType.ADD, unit_id=unit_id, content=content))
        else:
            # Placeholder for protocol-compliant LLM pipeline (unitize/ground/decide/commit).
            actions = []

        event_ids: list[str] = []
        for action in actions:
            event = self.ledger.append(scope=scope_norm, action=action, candidate_set_snapshot=None)
            event_ids.append(event.event_id)
            unit = self.ledger.get_unit(action.unit_id or "")
            if unit is not None:
                self.views.vector_view.upsert(unit)

        return {
            "actions": [asdict(a) for a in actions],
            "provenance": {
                "event_ids": event_ids,
                "unitize_count": len(actions),
            },
        }

    def recall(
        self,
        *,
        query: str,
        scope: dict[str, Any],
        limit: int = 5,
    ) -> dict[str, Any]:
        scope_norm = _normalize_scope(scope)
        if not scope_norm:
            raise ValueError("scope is required")

        result: RecallResult = self.views.vector_view.recall(
            query=query, scope_filter=scope_norm, limit=limit
        )

        units = []
        provenance = []
        for hit in result.hits:
            unit: MemoryUnit = hit.unit
            units.append(
                {
                    "id": unit.id,
                    "content": unit.content,
                    "metadata": unit.metadata,
                    "last_event_id": unit.last_event_id,
                }
            )
            provenance.append(
                {
                    "memory_id": unit.id,
                    "view_source": hit.view_source,
                    "last_event_id": unit.last_event_id,
                    "similarity": hit.similarity,
                }
            )

        return {"units": units, "provenance": provenance}
