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

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np


class ActionType(str, Enum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    NONE = "NONE"


@dataclass(frozen=True)
class Action:
    event_type: ActionType
    unit_id: str | None = None
    content: str | None = None
    previous_content: str | None = None


@dataclass
class MemoryUnit:
    id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    embedding: np.ndarray | None = None
    last_event_id: str | None = None


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    timestamp: float
    scope: dict[str, str]
    action: Action
    candidate_set_snapshot: list[str] | None = None

    @staticmethod
    def new_id() -> str:
        return f"evt_{uuid.uuid4().hex}"

    @staticmethod
    def now_ts() -> float:
        return time.time()


@dataclass(frozen=True)
class RecallHit:
    unit: MemoryUnit
    similarity: float
    view_source: str


@dataclass(frozen=True)
class RecallResult:
    hits: list[RecallHit]
