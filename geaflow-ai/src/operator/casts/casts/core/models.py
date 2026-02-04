"""Core data models for CASTS (Context-Aware Strategy Cache System)."""

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

# Filter out identity keys that should not participate in decision-making
IDENTITY_KEYS = {"id", "node_id", "uuid", "UID", "Uid", "Id"}


def filter_decision_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """Filter out identity fields from properties, keeping only decision-relevant attributes."""
    return {k: v for k, v in properties.items() if k not in IDENTITY_KEYS}


@dataclass
class Context:
    """Runtime context c = (structural_signature, properties, goal)
    
    Represents the current state of a graph traversal:
    - structural_signature: Current traversal path as a string (e.g., "V().out().in()")
    - properties: Current node properties (with identity fields filtered out)
    - goal: Natural language description of the traversal objective
    """
    structural_signature: str
    properties: dict[str, Any]
    goal: str

    @property
    def safe_properties(self) -> dict[str, Any]:
        """Return properties with identity fields removed for decision-making."""
        return filter_decision_properties(self.properties)


@dataclass
class StrategyKnowledgeUnit:
    """Strategy Knowledge Unit (SKU) - Core building block of the strategy cache.
    
    Mathematical definition:
    SKU = (context_template, decision_template, schema_fingerprint, 
           property_vector, confidence_score, logic_complexity)
    
    where context_template = (structural_signature, predicate, goal_template)
    
    Attributes:
        id: Unique identifier for this SKU
        structural_signature: s_sku - structural pattern that must match exactly
        predicate: Φ(p) - boolean function over properties
        goal_template: g_sku - goal pattern that must match exactly
        decision_template: d_template - traversal step template (e.g., "out('friend')")
        schema_fingerprint: ρ - schema version identifier
        property_vector: v_proto - embedding of properties at creation time
        confidence_score: η - dynamic confidence score (AIMD updated)
        logic_complexity: σ_logic - intrinsic logic complexity measure
    """
    id: str
    structural_signature: str
    predicate: Callable[[dict[str, Any]], bool]
    goal_template: str
    decision_template: str
    schema_fingerprint: str
    property_vector: np.ndarray
    confidence_score: float = 1.0
    logic_complexity: int = 1
    execution_count: int = 0

    def __hash__(self):
        return hash(self.id)

    @property
    def context_template(self) -> tuple[str, Callable[[dict[str, Any]], bool], str]:
        """Return the context template (s_sku, Φ, g_sku) as defined in the mathematical model."""
        return (self.structural_signature, self.predicate, self.goal_template)
