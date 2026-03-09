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

from fastapi.testclient import TestClient

from api.app import app


def _base_payload() -> dict:
    return {
        "goal": "find friends",
        "traversal": {"structural_signature": "V()", "step_index": 0},
        "node": {"label": "Person", "properties": {"type": "Person", "name": "Alice"}},
        "graph_schema": {
            "schema_fingerprint": "fp_test",
            "valid_outgoing_labels": ["friend"],
            "valid_incoming_labels": [],
        },
    }


def test_health_ok() -> None:
    client = TestClient(app)
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "UP"}


def test_empty_scope_rejected() -> None:
    client = TestClient(app)
    res = client.post(
        "/casts/decision",
        json={"api_version": "v1", "scope": {}, "trace": {}, "payload": _base_payload()},
    )
    assert res.status_code == 422
    body = res.json()
    assert body["ok"] is False


def test_missing_run_id_downgrades_to_stop() -> None:
    client = TestClient(app)
    res = client.post(
        "/casts/decision",
        json={
            "api_version": "v1",
            "scope": {"user_id": "u1"},
            "trace": {},
            "payload": _base_payload(),
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["payload"]["decision"] == "stop"
    assert body["payload"]["match_type"] == "STOP_INVALID"


def test_with_run_id_returns_a_decision() -> None:
    client = TestClient(app)
    res = client.post(
        "/casts/decision",
        json={
            "api_version": "v1",
            "scope": {"run_id": "run_test"},
            "trace": {},
            "payload": _base_payload(),
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    decision = body["payload"]["decision"]
    assert isinstance(decision, str)
    assert decision.strip()
