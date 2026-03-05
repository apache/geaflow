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

"""Graph schema implementation for CASTS system.

This module provides concrete schema implementations that decouple
graph structure metadata from execution logic.
"""

from enum import Enum

from core.constants import EDGE_LABEL_KEY, EDGE_TARGET_KEY, NODE_TYPE_KEY
from core.interfaces import GraphSchema
from core.types import GraphEdges, GraphNodes, JsonDict


class SchemaState(str, Enum):
    """Lifecycle state for schema extraction and validation."""

    DIRTY = "dirty"
    READY = "ready"


class InMemoryGraphSchema(GraphSchema):
    """In-memory implementation of GraphSchema for CASTS data sources."""

    def __init__(
        self, nodes: GraphNodes, edges: GraphEdges
    ):
        """Initialize schema from graph data.

        Args:
            nodes: Dictionary of node_id -> node_properties
            edges: Dictionary of source_node_id -> list of edge dicts
        """
        self._nodes = nodes
        self._edges = edges
        self._state = SchemaState.DIRTY
        self._reset_cache()
        self.rebuild()

    def mark_dirty(self) -> None:
        """Mark schema as dirty when underlying graph data changes."""
        self._state = SchemaState.DIRTY

    def rebuild(self) -> None:
        """Rebuild schema caches from the current graph data."""
        self._reset_cache()
        self._extract_schema()
        self._state = SchemaState.READY

    def _ensure_ready(self) -> None:
        """Ensure schema caches are initialized before read operations."""
        if self._state == SchemaState.DIRTY:
            self.rebuild()

    def _reset_cache(self) -> None:
        """Reset cached schema data structures."""
        self._node_types: set[str] = set()
        self._edge_labels: set[str] = set()
        self._node_type_schemas: dict[str, JsonDict] = {}
        self._node_edge_labels: dict[str, list[str]] = {}
        self._node_incoming_edge_labels: dict[str, list[str]] = {}
        self._node_type_outgoing_labels: dict[str, set[str]] = {}
        self._node_type_incoming_labels: dict[str, set[str]] = {}

    def _extract_schema(self) -> None:
        """Extract schema information from graph data."""
        for node_id, node_props in self._nodes.items():
            self._node_incoming_edge_labels[node_id] = []
            node_type = node_props.get(NODE_TYPE_KEY, "Unknown")
            self._node_type_outgoing_labels.setdefault(node_type, set())
            self._node_type_incoming_labels.setdefault(node_type, set())

        for source_id, out_edges in self._edges.items():
            if source_id in self._nodes:
                out_labels = sorted({edge[EDGE_LABEL_KEY] for edge in out_edges})
                self._node_edge_labels[source_id] = out_labels
                self._edge_labels.update(out_labels)
                source_type = self._nodes[source_id].get(NODE_TYPE_KEY, "Unknown")
                self._node_type_outgoing_labels.setdefault(source_type, set()).update(out_labels)

            for edge in out_edges:
                target_id = edge.get(EDGE_TARGET_KEY)
                if target_id and target_id in self._nodes:
                    self._node_incoming_edge_labels[target_id].append(edge[EDGE_LABEL_KEY])
                    target_type = self._nodes[target_id].get(NODE_TYPE_KEY, "Unknown")
                    self._node_type_incoming_labels.setdefault(target_type, set()).add(
                        edge[EDGE_LABEL_KEY]
                    )

        for node_id, incoming_labels in self._node_incoming_edge_labels.items():
            self._node_incoming_edge_labels[node_id] = sorted(set(incoming_labels))

        for node_id, node_props in self._nodes.items():
            node_type = node_props.get(NODE_TYPE_KEY, "Unknown")
            self._node_types.add(node_type)

            if node_type not in self._node_type_schemas:
                self._node_type_schemas[node_type] = {
                    "properties": {
                        key: type(value).__name__
                        for key, value in node_props.items()
                        if key not in {"id", "node_id", "uuid", "UID", "Uid", "Id"}
                    },
                    "example_node": node_id,
                }

    @property
    def node_types(self) -> set[str]:
        """Get all node types in the graph."""
        self._ensure_ready()
        return self._node_types.copy()

    @property
    def edge_labels(self) -> set[str]:
        """Get all edge labels in the graph."""
        self._ensure_ready()
        return self._edge_labels.copy()

    def get_node_schema(self, node_type: str) -> JsonDict:
        """Get schema information for a specific node type."""
        self._ensure_ready()
        return self._node_type_schemas.get(node_type, {}).copy()

    def get_valid_outgoing_edge_labels(self, node_type: str) -> list[str]:
        """Get valid outgoing edge labels for a node type."""
        self._ensure_ready()
        return sorted(self._node_type_outgoing_labels.get(node_type, set()))

    def get_valid_incoming_edge_labels(self, node_type: str) -> list[str]:
        """Get valid incoming edge labels for a node type."""
        self._ensure_ready()
        return sorted(self._node_type_incoming_labels.get(node_type, set()))

    def validate_edge_label(self, label: str) -> bool:
        """Validate if an edge label exists in the schema."""
        self._ensure_ready()
        return label in self._edge_labels

    def get_all_edge_labels(self) -> list[str]:
        """Get all edge labels as a list (for backward compatibility)."""
        self._ensure_ready()
        return list(self._edge_labels)
