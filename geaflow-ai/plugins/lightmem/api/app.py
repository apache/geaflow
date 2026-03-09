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

from typing import Any
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from lightmem.memory_kernel import MemoryKernel
from api.envelope import Envelope

app = FastAPI(title="GeaFlow AI LightMem", version="0.1.0")

_KERNEL = MemoryKernel()


class _Message(BaseModel):
    model_config = ConfigDict(extra="ignore")

    role: str
    content: str


class MemoryWritePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    messages: list[_Message] = Field(default_factory=list)
    mode: str | None = None


class MemoryRecallPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query: str
    limit: int | None = None


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    trace_id = f"tr_{uuid.uuid4().hex}"
    return JSONResponse(
        status_code=422,
        content={
            "ok": False,
            "error": {"code": "INVALID_REQUEST", "message": str(exc)},
            "trace": {"trace_id": trace_id},
        },
    )


@app.exception_handler(Exception)
async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    trace_id = f"tr_{uuid.uuid4().hex}"
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": f"internal error ({type(exc).__name__})",
            },
            "trace": {"trace_id": trace_id},
        },
    )


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "UP"}


@app.post("/memory/write")
async def memory_write(envelope: Envelope) -> JSONResponse:
    trace = envelope.trace.with_defaults()
    env = envelope.model_copy(update={"trace": trace})

    payload = MemoryWritePayload.model_validate(env.payload)
    result = _KERNEL.write(
        messages=[m.model_dump() for m in payload.messages],
        scope=env.scope.model_dump(exclude_none=True),
        mode=payload.mode or "echo",
    )
    provenance = dict(result.get("provenance") or {})
    provenance["trace_id"] = trace.trace_id

    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "api_version": env.api_version,
            "trace": trace.model_dump(),
            "payload": {"actions": result.get("actions") or [], "provenance": provenance},
        },
    )


@app.post("/memory/recall")
async def memory_recall(envelope: Envelope) -> JSONResponse:
    trace = envelope.trace.with_defaults()
    env = envelope.model_copy(update={"trace": trace})

    payload = MemoryRecallPayload.model_validate(env.payload)
    result = _KERNEL.recall(
        query=payload.query,
        scope=env.scope.model_dump(exclude_none=True),
        limit=payload.limit or 5,
    )
    result["trace_id"] = trace.trace_id

    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "api_version": env.api_version,
            "trace": trace.model_dump(),
            "payload": result,
        },
    )
