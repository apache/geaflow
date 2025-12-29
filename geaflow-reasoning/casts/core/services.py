"""Core strategy cache service for storing and retrieving traversal strategies."""

from typing import Any, List, Optional, Tuple

from casts.core.models import Context, StrategyKnowledgeUnit
from casts.utils.helpers import (
    calculate_dynamic_similarity_threshold,
    calculate_tier2_threshold,
    cosine_similarity,
)


class StrategyCache:
    """CASTS Strategy Cache for storing and matching traversal strategies (SKUs).

    Hyperparameters are aligned with the mathematical model described in
    `architecture.md` / `数学建模.md` and are configurable so that
    experiments can sweep over:

    - min_confidence_threshold (η_min): Tier 1 baseline confidence.
    - tier2_gamma (γ): Tier 2 confidence scaling factor,
        η_tier2(η_min) = γ · η_min.
    - similarity_kappa, similarity_beta: parameters of the dynamic
        similarity threshold δ_sim(v).
    """

    def __init__(self, embed_service: Any, config: Any):
        self.knowledge_base: List[StrategyKnowledgeUnit] = []
        self.embed_service = embed_service

        # Get all hyperparameters from the configuration object
        self.min_confidence_threshold = config.get_float("CACHE_MIN_CONFIDENCE_THRESHOLD", 2.0)
        self.current_schema_fingerprint = config.get_str("CACHE_SCHEMA_FINGERPRINT", "schema_v1")
        self.similarity_kappa = config.get_float("CACHE_SIMILARITY_KAPPA", 0.25)
        self.similarity_beta = config.get_float("CACHE_SIMILARITY_BETA", 0.05)
        self.tier2_gamma = config.get_float("CACHE_TIER2_GAMMA", 1.2)

    async def find_strategy(
        self,
        context: Context,
        skip_tier1: bool = False,
    ) -> Tuple[Optional[str], Optional[StrategyKnowledgeUnit], str]:
        """
        Find a matching strategy for the given context.

        Returns:
            Tuple of (decision_template, strategy_knowledge_unit, match_type)
            match_type: 'Tier1', 'Tier2', or None

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
                    sku.structural_signature == context.structural_signature
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
        # Compute Tier 2 confidence threshold η_tier2(η_min)
        tier2_confidence_threshold = calculate_tier2_threshold(
            self.min_confidence_threshold, self.tier2_gamma
        )

        for sku in self.knowledge_base:
            # Require exact match on structural signature, goal, and schema
            if (
                sku.structural_signature == context.structural_signature
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

    def add_sku(self, sku: StrategyKnowledgeUnit):
        """Add a new Strategy Knowledge Unit to the cache."""
        self.knowledge_base.append(sku)

    def update_confidence(self, sku: StrategyKnowledgeUnit, success: bool):
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
            sku.confidence_score = max(0.1, sku.confidence_score)

    def cleanup_low_confidence_skus(self):
        """Remove SKUs that have fallen below the minimum confidence threshold."""
        self.knowledge_base = [sku for sku in self.knowledge_base if sku.confidence_score >= 0.1]
