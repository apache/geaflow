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

"""Core data models for CASTS (Context-Aware Strategy Cache System)."""

from dataclasses import dataclass
from typing import Callable

import numpy as np

from core.types import JsonDict

# Filter out identity keys that should not participate in decision-making
IDENTITY_KEYS = {"id", "node_id", "uuid", "UID", "Uid", "Id"}


def filter_decision_properties(properties: JsonDict) -> JsonDict:
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
    properties: JsonDict
    goal: str

    @property
    def safe_properties(self) -> JsonDict:
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
        predicate: Phi(p) - boolean function over properties
        goal_template: g_sku - goal pattern that must match exactly
        decision_template: d_template - traversal step template (e.g., "out('friend')")
        schema_fingerprint: rho - schema version identifier
        property_vector: v_proto - embedding of properties at creation time
        confidence_score: eta - dynamic confidence score (AIMD updated)
        logic_complexity: sigma_logic - intrinsic logic complexity measure
    """

    id: str
    structural_signature: str
    predicate: Callable[[JsonDict], bool]
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
    def context_template(self) -> tuple[str, Callable[[JsonDict], bool], str]:
        """Return the context template (s_sku, Phi, g_sku) as defined in the model."""
        return (self.structural_signature, self.predicate, self.goal_template)
