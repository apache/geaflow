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

from core.interfaces import GraphSchema
from core.types import JsonDict


class RequestGraphSchema(GraphSchema):
    """GraphSchema adapter built from request payload metadata."""

    def __init__(
        self,
        *,
        schema_fingerprint: str,
        valid_outgoing_labels: list[str],
        valid_incoming_labels: list[str],
        node_types: set[str] | None = None,
        edge_labels: set[str] | None = None,
        node_schema: dict[str, Any] | None = None,
    ) -> None:
        self._schema_fingerprint = schema_fingerprint
        self._valid_outgoing_labels = list(valid_outgoing_labels)
        self._valid_incoming_labels = list(valid_incoming_labels)
        self._node_types = set(node_types or [])
        self._edge_labels = set(edge_labels or [])
        self._node_schema = dict(node_schema or {})

        if not self._edge_labels:
            self._edge_labels = set(self._valid_outgoing_labels) | set(self._valid_incoming_labels)

    @property
    def node_types(self) -> set[str]:
        return set(self._node_types)

    @property
    def edge_labels(self) -> set[str]:
        return set(self._edge_labels)

    def get_node_schema(self, node_type: str) -> JsonDict:
        if not node_type:
            return {}
        schema = self._node_schema.get(node_type)
        if isinstance(schema, dict):
            return schema
        return {}

    def get_valid_outgoing_edge_labels(self, node_type: str) -> list[str]:
        _ = node_type
        return list(self._valid_outgoing_labels)

    def get_valid_incoming_edge_labels(self, node_type: str) -> list[str]:
        _ = node_type
        return list(self._valid_incoming_labels)

    def validate_edge_label(self, label: str) -> bool:
        return label in self._edge_labels
