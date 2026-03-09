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
from typing import Any
import uuid

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Scope(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tenant_id: str | None = None
    user_id: str | None = None
    agent_id: str | None = None
    run_id: str | None = None
    actor_id: str | None = None

    @model_validator(mode="after")
    def _scope_required(self) -> Scope:
        has_any = any((self.tenant_id, self.user_id, self.agent_id, self.run_id, self.actor_id))
        if not has_any:
            raise ValueError("scope is required (tenant_id/user_id/agent_id/run_id/actor_id)")
        return self


class Trace(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trace_id: str | None = None
    timestamp: float | None = None
    caller: str | None = None

    def with_defaults(self) -> Trace:
        return Trace(
            trace_id=self.trace_id or f"tr_{uuid.uuid4().hex}",
            timestamp=self.timestamp if self.timestamp is not None else time.time(),
            caller=self.caller,
        )


class Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore")

    api_version: str = "v1"
    scope: Scope
    trace: Trace = Field(default_factory=Trace)
    payload: dict[str, Any] = Field(default_factory=dict)
