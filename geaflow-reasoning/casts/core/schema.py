"""Graph schema implementation for CASTS system.

This module provides concrete schema implementations that decouple
graph structure metadata from execution logic.
"""

from enum import Enum
from typing import Any, Dict, List, Set

from casts.core.interfaces import GraphSchema


class SchemaState(str, Enum):
    """Lifecycle state for schema extraction and validation."""

    DIRTY = "dirty"
    READY = "ready"


class InMemoryGraphSchema(GraphSchema):
    """In-memory implementation of GraphSchema for CASTS data sources."""

    def __init__(self, nodes: Dict[str, Dict[str, Any]], edges: Dict[str, List[Dict[str, str]]]):
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
        self._node_types: Set[str] = set()
        self._edge_labels: Set[str] = set()
        self._node_type_schemas: Dict[str, Dict[str, Any]] = {}
        self._node_edge_labels: Dict[str, List[str]] = {}
        self._node_incoming_edge_labels: Dict[str, List[str]] = {}

    def _extract_schema(self) -> None:
        """Extract schema information from graph data."""
        for node_id in self._nodes:
            self._node_incoming_edge_labels[node_id] = []

        for source_id, out_edges in self._edges.items():
            if source_id in self._nodes:
                out_labels = sorted({edge["label"] for edge in out_edges})
                self._node_edge_labels[source_id] = out_labels
                self._edge_labels.update(out_labels)

            for edge in out_edges:
                target_id = edge.get("target")
                if target_id and target_id in self._nodes:
                    self._node_incoming_edge_labels[target_id].append(edge["label"])

        for node_id, incoming_labels in self._node_incoming_edge_labels.items():
            self._node_incoming_edge_labels[node_id] = sorted(set(incoming_labels))

        for node_id, node_props in self._nodes.items():
            node_type = node_props.get("type", "Unknown")
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
    def node_types(self) -> Set[str]:
        """Get all node types in the graph."""
        self._ensure_ready()
        return self._node_types.copy()

    @property
    def edge_labels(self) -> Set[str]:
        """Get all edge labels in the graph."""
        self._ensure_ready()
        return self._edge_labels.copy()

    def get_node_schema(self, node_type: str) -> Dict[str, Any]:
        """Get schema information for a specific node type."""
        self._ensure_ready()
        return self._node_type_schemas.get(node_type, {}).copy()

    def get_valid_outgoing_edge_labels(self, node_id: str) -> List[str]:
        """Get valid outgoing edge labels for a specific node."""
        self._ensure_ready()
        return self._node_edge_labels.get(node_id, []).copy()

    def get_valid_incoming_edge_labels(self, node_id: str) -> List[str]:
        """Get valid incoming edge labels for a specific node."""
        self._ensure_ready()
        return self._node_incoming_edge_labels.get(node_id, []).copy()

    def validate_edge_label(self, label: str) -> bool:
        """Validate if an edge label exists in the schema."""
        self._ensure_ready()
        return label in self._edge_labels

    def get_all_edge_labels(self) -> List[str]:
        """Get all edge labels as a list (for backward compatibility)."""
        self._ensure_ready()
        return list(self._edge_labels)
