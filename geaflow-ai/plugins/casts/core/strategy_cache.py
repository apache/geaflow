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

"""Core strategy cache service for storing and retrieving traversal strategies."""

import re
from typing import Literal

from core.interfaces import Configuration, EmbeddingServiceProtocol
from core.models import Context, StrategyKnowledgeUnit
from utils.helpers import (
    calculate_dynamic_similarity_threshold,
    calculate_tier2_threshold,
    cosine_similarity,
)

MatchType = Literal["Tier1", "Tier2", ""]


class StrategyCache:
    """CASTS Strategy Cache for storing and matching traversal strategies (SKUs).

    Implements the two-tier matching system described in the project documentation:
        - Tier 1 (Strict Logic): Exact structural + goal match with predicate Phi(p)
        - Tier 2 (Similarity): Embedding-based fallback with adaptive threshold

    Mathematical model alignment:
        - Tier 1 candidates: C_strict(c) where eta >= eta_min
        - Tier 2 candidates: C_sim(c) where eta >= eta_tier2(eta_min) = gamma * eta_min
        - Similarity threshold: delta_sim(v) = 1 - kappa / (sigma_logic * (1 + beta * log(eta)))

    Hyperparameters (configurable for experiments):
        - min_confidence_threshold (eta_min): Tier 1 baseline confidence
        - tier2_gamma (gamma): Tier 2 confidence scaling factor (gamma > 1)
        - similarity_kappa (kappa): Base threshold sensitivity
        - similarity_beta (beta): Frequency sensitivity

    Note: Higher eta (confidence) -> higher delta_sim -> stricter matching requirement
    """

    def __init__(self, embed_service: EmbeddingServiceProtocol, config: Configuration):
        self.knowledge_base: list[StrategyKnowledgeUnit] = []
        self.embed_service = embed_service

        # Get all hyperparameters from the configuration object
        # Default values balance exploration and safety (see config.py for detailed rationale)
        # Note: Higher kappa -> lower threshold -> more permissive (counter-intuitive!)
        self.min_confidence_threshold = config.get_float("CACHE_MIN_CONFIDENCE_THRESHOLD")
        self.current_schema_fingerprint = config.get_str("CACHE_SCHEMA_FINGERPRINT")
        self.similarity_kappa = config.get_float("CACHE_SIMILARITY_KAPPA")
        self.similarity_beta = config.get_float("CACHE_SIMILARITY_BETA")
        self.tier2_gamma = config.get_float("CACHE_TIER2_GAMMA")
        self.signature_level = config.get_int("SIGNATURE_LEVEL")
        default_edge_whitelist: list[str] | None = None
        self.edge_whitelist = config.get("SIGNATURE_EDGE_WHITELIST", default_edge_whitelist)
        self.min_execution_confidence = config.get_float("MIN_EXECUTION_CONFIDENCE")

    async def find_strategy(
        self,
        context: Context,
        skip_tier1: bool = False,
    ) -> tuple[str | None, StrategyKnowledgeUnit | None, MatchType]:
        """
        Find a matching strategy for the given context.

        Returns:
            Tuple of (decision_template, strategy_knowledge_unit, match_type)
            match_type: 'Tier1', 'Tier2', or ''

        Two-tier matching:
        - Tier 1: Strict logic matching (exact structural signature, goal, schema, and predicate)
        - Tier 2: Similarity-based fallback (vector similarity when Tier 1 fails)
        """
        # Tier 1: Strict Logic Matching
        tier1_candidates = []
        if not skip_tier1:  # Can bypass Tier1 for testing
            for sku in self.knowledge_base:
                # Exact matching on structural signature, goal, and schema
                if (
                    self._signatures_match(context.structural_signature, sku.structural_signature)
                    and sku.goal_template == context.goal
                    and sku.schema_fingerprint == self.current_schema_fingerprint
                ):
                    # Predicate only uses safe properties (no identity fields)
                    try:
                        if sku.confidence_score >= self.min_confidence_threshold and sku.predicate(
                            context.safe_properties
                        ):
                            tier1_candidates.append(sku)
                    except (KeyError, TypeError, ValueError, AttributeError) as e:
                        # Defensive: some predicates may error on missing fields
                        print(f"[warn] Tier1 predicate error on SKU {sku.id}: {e}")
                        continue

        if tier1_candidates:
            # Pick best by confidence score
            best_sku = max(tier1_candidates, key=lambda x: x.confidence_score)
            return best_sku.decision_template, best_sku, "Tier1"

        # Tier 2: Similarity-based Fallback (only if Tier 1 fails)
        tier2_candidates = []
        # Vector embedding based on safe properties only
        property_vector = await self.embed_service.embed_properties(context.safe_properties)
        # Compute Tier 2 confidence threshold eta_tier2(eta_min)
        tier2_confidence_threshold = calculate_tier2_threshold(
            self.min_confidence_threshold, self.tier2_gamma
        )

        for sku in self.knowledge_base:
            # Require exact match on structural signature, goal, and schema
            if (
                self._signatures_match(context.structural_signature, sku.structural_signature)
                and sku.goal_template == context.goal
                and sku.schema_fingerprint == self.current_schema_fingerprint
            ):
                if sku.confidence_score >= tier2_confidence_threshold:  # Higher bar for Tier 2
                    similarity = cosine_similarity(property_vector, sku.property_vector)
                    threshold = calculate_dynamic_similarity_threshold(
                        sku, self.similarity_kappa, self.similarity_beta
                    )
                    print(
                        f"[debug] SKU {sku.id} - similarity: {similarity:.4f}, "
                        f"threshold: {threshold:.4f}"
                    )
                    if similarity >= threshold:
                        tier2_candidates.append((sku, similarity))

        if tier2_candidates:
            # Rank by confidence score primarily
            best_sku, similarity = max(tier2_candidates, key=lambda x: x[0].confidence_score)
            return best_sku.decision_template, best_sku, "Tier2"

        # Explicitly type-safe None return for all components
        return None, None, ""

    def _to_abstract_signature(self, signature: str) -> str:
        """Convert a canonical Level-2 signature to the configured abstraction level."""
        if self.signature_level == 2:
            return signature

        abstract_parts = []
        steps = signature.split('.')
        for i, step in enumerate(steps):
            if i == 0:
                abstract_parts.append(step)
                continue

            match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)(\(.*\))?", step)
            if not match:
                abstract_parts.append(step)
                continue

            op = match.group(1)
            params = match.group(2) or "()"

            # Level 0: Abstract everything
            if self.signature_level == 0:
                if op in ["out", "in", "both", "outE", "inE", "bothE"]:
                    base_op = op.replace("E", "").replace("V", "")
                    abstract_parts.append(f"{base_op}()")
                else:
                    abstract_parts.append("filter()")
                continue

            # Level 1: Edge-aware
            if self.signature_level == 1:
                if op in ["out", "in", "both", "outE", "inE", "bothE"]:
                    if self.edge_whitelist:
                        label_match = re.search(r"\('([^']+)'\)", params)
                        if label_match and label_match.group(1) in self.edge_whitelist:
                            abstract_parts.append(step)
                        else:
                            base_op = op.replace("E", "").replace("V", "")
                            abstract_parts.append(f"{base_op}()")
                    else:
                        abstract_parts.append(step)
                else:
                    abstract_parts.append("filter()")

        return ".".join(abstract_parts)

    def _signatures_match(self, runtime_sig: str, stored_sig: str) -> bool:
        """Check if two canonical signatures match at the configured abstraction level."""
        runtime_abstract = self._to_abstract_signature(runtime_sig)
        stored_abstract = self._to_abstract_signature(stored_sig)
        return runtime_abstract == stored_abstract

    def add_sku(self, sku: StrategyKnowledgeUnit) -> None:
        """Add a new Strategy Knowledge Unit to the cache."""
        self.knowledge_base.append(sku)

    def update_confidence(self, sku: StrategyKnowledgeUnit, success: bool) -> None:
        """
        Update confidence score using AIMD (Additive Increase, Multiplicative Decrease).

        Args:
            sku: The strategy knowledge unit to update
            success: Whether the strategy execution was successful
        """
        if success:
            # Additive increase
            sku.confidence_score += 1.0
        else:
            # Multiplicative decrease (penalty)
            sku.confidence_score *= 0.5
            # Ensure confidence doesn't drop below minimum
            sku.confidence_score = max(self.min_execution_confidence, sku.confidence_score)

    def cleanup_low_confidence_skus(self) -> None:
        """Remove SKUs that have fallen below the minimum confidence threshold."""
        self.knowledge_base = [
            sku
            for sku in self.knowledge_base
            if sku.confidence_score >= self.min_confidence_threshold
        ]
