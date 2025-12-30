"""Core interfaces and abstractions for CASTS system.

This module defines the key abstractions that enable dependency injection
and adherence to SOLID principles, especially Dependency Inversion Principle (DIP).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple

import numpy as np


class GoalGenerator(ABC):
    """Abstract interface for generating traversal goals based on graph schema."""

    @property
    @abstractmethod
    def goal_texts(self) -> List[str]:
        """Get list of available goal descriptions."""
        pass

    @property
    @abstractmethod
    def goal_weights(self) -> List[int]:
        """Get weights for goal selection (higher = more frequent)."""
        pass

    @abstractmethod
    def select_goal(self, node_type: Optional[str] = None) -> Tuple[str, str]:
        """Select a goal based on weights and optional node type context.

        Returns:
            Tuple of (goal_text, evaluation_rubric)
        """
        pass


class GraphSchema(ABC):
    """Abstract interface for graph schema describing structural constraints."""

    @property
    @abstractmethod
    def node_types(self) -> Set[str]:
        """Get all node types in the graph."""
        pass

    @property
    @abstractmethod
    def edge_labels(self) -> Set[str]:
        """Get all edge labels in the graph."""
        pass

    @abstractmethod
    def get_node_schema(self, node_type: str) -> Dict[str, Any]:
        """Get schema information for a specific node type."""
        pass

    @abstractmethod
    def get_valid_outgoing_edge_labels(self, node_id: str) -> List[str]:
        """Get valid outgoing edge labels for a specific node."""
        pass

    @abstractmethod
    def get_valid_incoming_edge_labels(self, node_id: str) -> List[str]:
        """Get valid incoming edge labels for a specific node."""
        pass

    @abstractmethod
    def validate_edge_label(self, label: str) -> bool:
        """Validate if an edge label exists in the schema."""
        pass


class DataSource(ABC):
    """Abstract interface for graph data sources.

    This abstraction allows the system to work with both synthetic and real data
    without coupling to specific implementations.
    """

    @property
    @abstractmethod
    def nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get all nodes in the graph."""
        pass

    @property
    @abstractmethod
    def edges(self) -> Dict[str, List[Dict[str, str]]]:
        """Get all edges in the graph."""
        pass

    @property
    @abstractmethod
    def source_label(self) -> str:
        """Get label identifying the data source type."""
        pass

    @abstractmethod
    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific node by ID."""
        pass

    @abstractmethod
    def get_neighbors(self, node_id: str, edge_label: Optional[str] = None) -> List[str]:
        """Get neighbor node IDs for a given node."""
        pass

    @abstractmethod
    def get_schema(self) -> GraphSchema:
        """Get the graph schema for this data source."""
        pass

    @abstractmethod
    def get_goal_generator(self) -> GoalGenerator:
        """Get the goal generator for this data source."""
        pass


class EmbeddingServiceProtocol(Protocol):
    """Protocol for embedding services (structural typing)."""

    async def embed_text(self, text: str) -> np.ndarray:
        """Generate embedding for text."""

    async def embed_properties(self, properties: Dict[str, Any]) -> np.ndarray:
        """Generate embedding for property dictionary."""


class LLMServiceProtocol(Protocol):
    """Protocol for LLM services (structural typing)."""

    async def generate_strategy(self, context: Dict[str, Any]) -> str:
        """Generate traversal strategy for given context."""

    async def generate_sku(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate Strategy Knowledge Unit for given context."""


class Configuration(ABC):
    """Abstract interface for configuration management."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""

    @abstractmethod
    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""

    @abstractmethod
    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value."""
        pass

    @abstractmethod
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        pass

    @abstractmethod
    def get_str(self, key: str, default: str = "") -> str:
        """Get string configuration value."""
        pass

    @abstractmethod
    def get_llm_config(self) -> Dict[str, str]:
        """Get LLM service configuration."""
        pass
