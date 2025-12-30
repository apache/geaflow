"""Graph schema implementation for CASTS system.

This module provides concrete schema implementations that decouple
graph structure metadata from execution logic.
"""

from typing import Any, Dict, List, Set

from casts.core.interfaces import GraphSchema


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
        self._node_types: Set[str] = set()
        self._edge_labels: Set[str] = set()
        self._node_type_schemas: Dict[str, Dict[str, Any]] = {}
        self._node_edge_labels: Dict[str, List[str]] = {}
        self._node_incoming_edge_labels: Dict[str, List[str]] = {}
        
        self._extract_schema()
    
    def _extract_schema(self) -> None:
        """Extract schema information from graph data."""
        # Pre-initialize all nodes with empty lists for incoming edges
        for node_id in self._nodes:
            self._node_incoming_edge_labels[node_id] = []

        # Extract outgoing and incoming edge labels
        for source_id, out_edges in self._edges.items():
            # Process outgoing edges for the source node
            if source_id in self._nodes:
                out_labels = list({edge["label"] for edge in out_edges})
                self._node_edge_labels[source_id] = out_labels
                self._edge_labels.update(out_labels)

            # Process incoming edges for the target nodes
            for edge in out_edges:
                target_id = edge.get("target")
                if target_id and target_id in self._nodes:
                    self._node_incoming_edge_labels[target_id].append(edge["label"])

        # Remove duplicates from incoming labels
        for node in self._node_incoming_edge_labels.items():
            self._node_incoming_edge_labels[node[0]] = sorted(set(node[1]))

        # Original node type and property schema extraction logic
        for node_id, node_props in self._nodes.items():
            node_type = node_props.get('type', 'Unknown')
            self._node_types.add(node_type)

            # Build property schema for this node type (sample first occurrence)
            if node_type not in self._node_type_schemas:
                self._node_type_schemas[node_type] = {
                    "properties": {
                        k: type(v).__name__
                        for k, v in node_props.items()
                        if k not in {"id", "node_id", "uuid", "UID", "Uid", "Id"}
                    },
                    "example_node": node_id,
                }

    @property
    def node_types(self) -> Set[str]:
        """Get all node types in the graph."""
        return self._node_types.copy()

    @property
    def edge_labels(self) -> Set[str]:
        """Get all edge labels in the graph."""
        return self._edge_labels.copy()

    def get_node_schema(self, node_type: str) -> Dict[str, Any]:
        """Get schema information for a specific node type."""
        return self._node_type_schemas.get(node_type, {}).copy()

    def get_valid_outgoing_edge_labels(self, node_id: str) -> List[str]:
        """Get valid outgoing edge labels for a specific node."""
        return self._node_edge_labels.get(node_id, []).copy()

    def get_valid_incoming_edge_labels(self, node_id: str) -> List[str]:
        """Get valid incoming edge labels for a specific node."""
        return self._node_incoming_edge_labels.get(node_id, []).copy()
    
    def validate_edge_label(self, label: str) -> bool:
        """Validate if an edge label exists in the schema."""
        return label in self._edge_labels
    
    def get_all_edge_labels(self) -> List[str]:
        """Get all edge labels as a list (for backward compatibility)."""
        return list(self._edge_labels)
