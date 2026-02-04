"""Core interfaces and abstractions for CASTS system.

This module defines the key abstractions that enable dependency injection
and adherence to SOLID principles, especially Dependency Inversion Principle (DIP).
"""

from abc import ABC, abstractmethod
from typing import Any, Protocol

import numpy as np


class GoalGenerator(ABC):
    """Abstract interface for generating traversal goals based on graph schema."""

    @property
    @abstractmethod
    def goal_texts(self) -> list[str]:
        """Get list of available goal descriptions."""
        pass

    @property
    @abstractmethod
    def goal_weights(self) -> list[int]:
        """Get weights for goal selection (higher = more frequent)."""
        pass

    @abstractmethod
    def select_goal(self, node_type: str | None = None) -> tuple[str, str]:
        """Select a goal based on weights and optional node type context.

        Returns:
            Tuple of (goal_text, evaluation_rubric)
        """
        pass


class GraphSchema(ABC):
    """Abstract interface for graph schema describing structural constraints."""

    @property
    @abstractmethod
    def node_types(self) -> set[str]:
        """Get all node types in the graph."""
        pass

    @property
    @abstractmethod
    def edge_labels(self) -> set[str]:
        """Get all edge labels in the graph."""
        pass

    @abstractmethod
    def get_node_schema(self, node_type: str) -> dict[str, Any]:
        """Get schema information for a specific node type."""
        pass

    @abstractmethod
    def get_valid_outgoing_edge_labels(self, node_id: str) -> list[str]:
        """Get valid outgoing edge labels for a specific node."""
        pass

    @abstractmethod
    def get_valid_incoming_edge_labels(self, node_id: str) -> list[str]:
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
    def nodes(self) -> dict[str, dict[str, Any]]:
        """Get all nodes in the graph."""
        pass

    @property
    @abstractmethod
    def edges(self) -> dict[str, list[dict[str, str]]]:
        """Get all edges in the graph."""
        pass

    @property
    @abstractmethod
    def source_label(self) -> str:
        """Get label identifying the data source type."""
        pass

    @abstractmethod
    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Get a specific node by ID."""
        pass

    @abstractmethod
    def get_neighbors(self, node_id: str, edge_label: str | None = None) -> list[str]:
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

    @abstractmethod
    def get_starting_nodes(
        self,
        goal: str,
        recommended_node_types: list[str],
        count: int,
        min_degree: int = 2,
    ) -> list[str]:
        """Select appropriate starting nodes for traversal.

        Implements a multi-tier selection strategy:
        1. Tier 1: Prefer nodes matching recommended_node_types
        2. Tier 2: Fallback to nodes with at least min_degree outgoing edges
        3. Tier 3: Emergency fallback to any available nodes

        Args:
            goal: The traversal goal text (for logging/debugging)
            recommended_node_types: List of node types recommended by LLM
            count: Number of starting nodes to return
            min_degree: Minimum outgoing degree for fallback selection

        Returns:
            List of node IDs suitable for starting traversal
        """
        pass


class EmbeddingServiceProtocol(Protocol):
    """Protocol for embedding services (structural typing)."""

    async def embed_text(self, text: str) -> np.ndarray:
        """Generate embedding for text."""

    async def embed_properties(self, properties: dict[str, Any]) -> np.ndarray:
        """Generate embedding for property dictionary."""


class LLMServiceProtocol(Protocol):
    """Protocol for LLM services (structural typing)."""

    async def generate_strategy(self, context: dict[str, Any]) -> str:
        """Generate traversal strategy for given context."""

    async def generate_sku(self, context: dict[str, Any]) -> dict[str, Any]:
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
    def get_llm_config(self) -> dict[str, str]:
        """Get LLM service configuration."""
        pass
