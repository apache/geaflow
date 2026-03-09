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

from api.envelope import Envelope
from api.service import decide

app = FastAPI(title="GeaFlow AI CASTS", version="0.1.0")


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


@app.post("/casts/decision")
async def casts_decision(envelope: Envelope) -> JSONResponse:
    trace = envelope.trace.with_defaults()
    env = envelope.model_copy(update={"trace": trace})

    payload = await decide(env)
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "api_version": env.api_version,
            "trace": trace.model_dump(),
            "payload": payload,
        },
    )
