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
        
        self._extract_schema()
    
    def _extract_schema(self) -> None:
        """Extract schema information from graph data."""
        # Extract node types and their property schemas
        for node_id, node_props in self._nodes.items():
            node_type = node_props.get('type', 'Unknown')
            self._node_types.add(node_type)
            
            # Build property schema for this node type (sample first occurrence)
            if node_type not in self._node_type_schemas:
                self._node_type_schemas[node_type] = {
                    'properties': {k: type(v).__name__ for k, v in node_props.items() 
                                 if k not in {'id', 'node_id', 'uuid', 'UID', 'Uid', 'Id'}},
                    'example_node': node_id
                }
            
            # Extract valid edge labels for this node
            if node_id in self._edges:
                valid_labels = list({edge['label'] for edge in self._edges[node_id]})
                self._node_edge_labels[node_id] = valid_labels
                self._edge_labels.update(valid_labels)
    
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
    
    def get_valid_edge_labels(self, node_id: str) -> List[str]:
        """Get valid edge labels for a specific node."""
        return self._node_edge_labels.get(node_id, []).copy()
    
    def validate_edge_label(self, label: str) -> bool:
        """Validate if an edge label exists in the schema."""
        return label in self._edge_labels
    
    def get_all_edge_labels(self) -> List[str]:
        """Get all edge labels as a list (for backward compatibility)."""
        return list(self._edge_labels)
