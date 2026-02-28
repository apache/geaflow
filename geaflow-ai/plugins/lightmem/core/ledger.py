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

from dataclasses import replace
from typing import Iterable

from lightmem.types import Action, LedgerEvent, MemoryUnit


class Ledger:
    """Append-only event log. Authoritative source of truth."""

    def __init__(self) -> None:
        self._events: list[LedgerEvent] = []
        self._units: dict[str, MemoryUnit] = {}

    def append(
        self,
        *,
        scope: dict[str, str],
        action: Action,
        candidate_set_snapshot: list[str] | None = None,
    ) -> LedgerEvent:
        event = LedgerEvent(
            event_id=LedgerEvent.new_id(),
            timestamp=LedgerEvent.now_ts(),
            scope=dict(scope),
            action=action,
            candidate_set_snapshot=list(candidate_set_snapshot) if candidate_set_snapshot else None,
        )
        self._events.append(event)

        if action.event_type == action.event_type.ADD:
            if not action.unit_id or not action.content:
                raise ValueError("ADD requires unit_id and content")
            unit = MemoryUnit(
                id=action.unit_id,
                content=action.content,
                metadata={"scope": dict(scope)},
                last_event_id=event.event_id,
            )
            self._units[unit.id] = unit
        elif action.event_type == action.event_type.UPDATE:
            if not action.unit_id or action.content is None:
                raise ValueError("UPDATE requires unit_id and content")
            existing = self._units.get(action.unit_id)
            if existing is None:
                raise KeyError(f"unknown unit_id={action.unit_id}")
            updated = replace(existing, content=action.content, last_event_id=event.event_id)
            self._units[updated.id] = updated
        elif action.event_type == action.event_type.DELETE:
            if not action.unit_id:
                raise ValueError("DELETE requires unit_id")
            self._units.pop(action.unit_id, None)
        elif action.event_type == action.event_type.NONE:
            pass

        return event

    def get_unit(self, unit_id: str) -> MemoryUnit | None:
        return self._units.get(unit_id)

    def list_units(self) -> list[MemoryUnit]:
        return list(self._units.values())

    def iter_events(self) -> Iterable[LedgerEvent]:
        return iter(self._events)
