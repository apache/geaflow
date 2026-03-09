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

from fastapi.testclient import TestClient

from api.app import app


def test_health_ok() -> None:
    client = TestClient(app)
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "UP"}


def test_empty_scope_rejected() -> None:
    client = TestClient(app)
    res = client.post(
        "/memory/recall",
        json={
            "api_version": "v1",
            "scope": {},
            "trace": {},
            "payload": {"query": "hi", "limit": 5},
        },
    )
    assert res.status_code == 422
    body = res.json()
    assert body["ok"] is False


def test_write_then_recall() -> None:
    user_id = f"u_{uuid.uuid4().hex}"
    client = TestClient(app)

    write_res = client.post(
        "/memory/write",
        json={
            "api_version": "v1",
            "scope": {"user_id": user_id},
            "trace": {},
            "payload": {
                "mode": "echo",
                "messages": [{"role": "user", "content": "I like coffee."}],
            },
        },
    )
    assert write_res.status_code == 200
    write_body = write_res.json()
    assert write_body["ok"] is True
    assert write_body["payload"]["actions"]

    recall_res = client.post(
        "/memory/recall",
        json={
            "api_version": "v1",
            "scope": {"user_id": user_id},
            "trace": {},
            "payload": {"query": "coffee", "limit": 5},
        },
    )
    assert recall_res.status_code == 200
    recall_body = recall_res.json()
    assert recall_body["ok"] is True
    assert recall_body["payload"]["units"]
