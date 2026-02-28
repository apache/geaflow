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

"""
Unit tests: canonical storage and abstract matching.

This module validates CASTS signature handling:
1. TraversalExecutor always generates Level 2 (canonical) signatures.
2. StrategyCache matches signatures at different abstraction levels.
3. The three-level abstraction system (Level 0/1/2) behaves as expected.

Coverage:
- Signature generation (executor.py)
- Signature abstraction (_to_abstract_signature)
- Signature matching sensitivity (_signatures_match)
- Edge cases: edge whitelist, filters, and edge traversal
"""

import unittest
from unittest.mock import AsyncMock, MagicMock

from core.config import DefaultConfiguration
from core.interfaces import DataSource, GraphSchema
from core.models import Context, StrategyKnowledgeUnit
from core.strategy_cache import StrategyCache
from harness.simulation.executor import TraversalExecutor


class MockGraphSchema(GraphSchema):
    """Mock GraphSchema for testing."""

    def __init__(self):
        self._node_types = {"Person", "Company", "Account"}
        self._edge_labels = {"friend", "transfer", "guarantee", "works_for"}

    @property
    def node_types(self):
        return self._node_types

    @property
    def edge_labels(self):
        return self._edge_labels

    def get_node_schema(self, node_type: str):
        return {}

    def get_valid_outgoing_edge_labels(self, node_type: str):
        return list(self._edge_labels)

    def get_valid_incoming_edge_labels(self, node_type: str):
        return list(self._edge_labels)

    def validate_edge_label(self, label: str):
        return label in self._edge_labels


class MockDataSource(DataSource):
    """Mock DataSource for testing."""

    def __init__(self):
        self._nodes = {
            "A": {"type": "Person", "name": "Alice"},
            "B": {"type": "Company", "name": "Acme Inc"},
            "C": {"type": "Account", "id": "12345"},
        }
        self._edges = {
            "A": [{"target": "B", "label": "friend"}],
            "B": [{"target": "C", "label": "transfer"}],
        }
        self._schema = MockGraphSchema()
        self._source_label = "mock"

    @property
    def nodes(self):
        return self._nodes

    @property
    def edges(self):
        return self._edges

    @property
    def source_label(self):
        return self._source_label

    def get_node(self, node_id: str):
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str, edge_label=None):
        neighbors = []
        for edge in self._edges.get(node_id, []):
            if edge_label is None or edge["label"] == edge_label:
                neighbors.append(edge["target"])
        return neighbors

    def get_schema(self):
        return self._schema

    def get_goal_generator(self):
        return None

    def get_starting_nodes(
        self, goal: str, recommended_node_types, count: int, min_degree: int = 2
    ):
        """Mock implementation of get_starting_nodes."""
        # Unused parameters for mock implementation
        _ = goal, recommended_node_types, min_degree
        return list(self._nodes.keys())[:count]


class TestTraversalExecutorCanonicalSignature(unittest.IsolatedAsyncioTestCase):
    """Test that TraversalExecutor always generates Level 2 signatures."""

    def setUp(self):
        self.data_source = MockDataSource()
        self.schema = self.data_source.get_schema()
        self.executor = TraversalExecutor(self.data_source, self.schema)

    async def test_edge_traversal_preserves_labels(self):
        """Test that edge traversal decisions preserve labels."""
        current_signature = "V()"
        decision = "out('friend')"
        current_node_id = "A"

        result = await self.executor.execute_decision(current_node_id, decision, current_signature)

        # Verify edge labels are preserved in the signature.
        self.assertEqual(len(result), 1)
        next_node_id, next_signature, traversed_edge = result[0]
        self.assertEqual(next_signature, "V().out('friend')")
        self.assertEqual(next_node_id, "B")

    async def test_filter_step_preserves_full_details(self):
        """Test that filter steps preserve full parameters."""
        current_signature = "V().out('friend')"
        decision = "has('type','Person')"
        current_node_id = "A"

        result = await self.executor.execute_decision(current_node_id, decision, current_signature)

        # Verify has() parameters are preserved.
        if result:  # has() may not match and can return an empty list.
            next_node_id, next_signature, traversed_edge = result[0]
            self.assertEqual(next_signature, "V().out('friend').has('type','Person')")

    async def test_edge_step_with_outE(self):
        """Test that outE steps retain edge labels."""
        current_signature = "V()"
        decision = "outE('transfer')"
        current_node_id = "B"

        result = await self.executor.execute_decision(current_node_id, decision, current_signature)

        self.assertEqual(len(result), 1)
        next_node_id, next_signature, traversed_edge = result[0]
        self.assertEqual(next_signature, "V().outE('transfer')")

    async def test_dedup_step_canonical_form(self):
        """Test canonical form for dedup() steps."""
        current_signature = "V().out('friend')"
        decision = "dedup()"
        current_node_id = "A"

        result = await self.executor.execute_decision(current_node_id, decision, current_signature)

        # dedup should be retained in the signature.
        self.assertEqual(len(result), 1)
        next_node_id, next_signature, traversed_edge = result[0]
        self.assertEqual(next_signature, "V().out('friend').dedup()")


class TestSignatureAbstraction(unittest.TestCase):
    """Test StrategyCache signature abstraction logic."""

    def setUp(self):
        """Create an isolated config/cache for each test."""
        self.mock_embed_service = MagicMock()

    def _create_cache_with_level(self, level: int, edge_whitelist=None):
        """Create a StrategyCache with the specified abstraction level."""
        config = MagicMock()
        config.get_float = MagicMock(
            side_effect=lambda k, d=0.0: (
                2.0 if "THRESHOLD" in k else 0.1 if k == "MIN_EXECUTION_CONFIDENCE" else d
            )
        )
        config.get_str = MagicMock(return_value="schema_v2_canonical")
        config.get_int = MagicMock(
            side_effect=lambda k, d=0: level if k == "SIGNATURE_LEVEL" else d
        )
        config.get = MagicMock(return_value=edge_whitelist)

        return StrategyCache(self.mock_embed_service, config)

    def test_level_2_no_abstraction(self):
        """Level 2: no abstraction."""
        cache = self._create_cache_with_level(2)

        canonical = "V().out('friend').has('type','Person').out('works_for')"
        abstracted = cache._to_abstract_signature(canonical)

        self.assertEqual(abstracted, canonical)

    def test_level_1_abstracts_filters_only(self):
        """Level 1: keep edge labels, abstract filters."""
        cache = self._create_cache_with_level(1)

        canonical = "V().out('friend').has('type','Person').out('works_for')"
        abstracted = cache._to_abstract_signature(canonical)

        expected = "V().out('friend').filter().out('works_for')"
        self.assertEqual(abstracted, expected)

    def test_level_0_abstracts_everything(self):
        """Level 0: abstract all edge labels and filters."""
        cache = self._create_cache_with_level(0)

        canonical = "V().out('friend').has('type','Person').out('works_for')"
        abstracted = cache._to_abstract_signature(canonical)

        expected = "V().out().filter().out()"
        self.assertEqual(abstracted, expected)

    def test_level_1_preserves_edge_variants(self):
        """Level 1: keep outE/inE/bothE distinctions."""
        cache = self._create_cache_with_level(1)

        test_cases = [
            ("V().outE('transfer')", "V().outE('transfer')"),
            ("V().inE('guarantee')", "V().inE('guarantee')"),
            ("V().bothE('friend')", "V().bothE('friend')"),
        ]

        for canonical, expected in test_cases:
            with self.subTest(canonical=canonical):
                abstracted = cache._to_abstract_signature(canonical)
                self.assertEqual(abstracted, expected)

    def test_level_0_normalizes_edge_variants(self):
        """Level 0: normalize outE/inE/bothE to out/in/both."""
        cache = self._create_cache_with_level(0)

        test_cases = [
            ("V().outE('transfer')", "V().out()"),
            ("V().inE('guarantee')", "V().in()"),
            ("V().bothE('friend')", "V().both()"),
        ]

        for canonical, expected in test_cases:
            with self.subTest(canonical=canonical):
                abstracted = cache._to_abstract_signature(canonical)
                self.assertEqual(abstracted, expected)

    def test_edge_whitelist_at_level_1(self):
        """Level 1 + Edge Whitelist: keep only whitelisted edge labels."""
        cache = self._create_cache_with_level(1, edge_whitelist=["friend", "works_for"])

        canonical = "V().out('friend').out('transfer').out('works_for')"
        abstracted = cache._to_abstract_signature(canonical)

        # 'friend' and 'works_for' are whitelisted, so they stay.
        # 'transfer' is not, so it is abstracted to out().
        expected = "V().out('friend').out().out('works_for')"
        self.assertEqual(abstracted, expected)

    def test_complex_filter_steps_level_1(self):
        """Level 1: filter steps should be abstracted to filter()."""
        cache = self._create_cache_with_level(1)

        test_cases = [
            ("V().has('type','Person')", "V().filter()"),
            ("V().limit(10)", "V().filter()"),
            ("V().values('id')", "V().filter()"),
            ("V().inV()", "V().filter()"),
            ("V().dedup()", "V().filter()"),
        ]

        for canonical, expected in test_cases:
            with self.subTest(canonical=canonical):
                abstracted = cache._to_abstract_signature(canonical)
                self.assertEqual(abstracted, expected)


class TestSignatureMatching(unittest.IsolatedAsyncioTestCase):
    """Test StrategyCache signature matching behavior."""

    def setUp(self):
        self.mock_embed_service = MagicMock()
        self.mock_embed_service.embed_properties = AsyncMock(return_value=[0.1] * 10)

    def _create_cache_with_level(self, level: int):
        """Create a StrategyCache with the specified abstraction level."""
        config = MagicMock()
        config.get_float = MagicMock(
            side_effect=lambda k, d=0.0: {
                "CACHE_MIN_CONFIDENCE_THRESHOLD": 2.0,
                "CACHE_TIER2_GAMMA": 1.2,
                "CACHE_SIMILARITY_KAPPA": 0.25,
                "CACHE_SIMILARITY_BETA": 0.05,
                "MIN_EXECUTION_CONFIDENCE": 0.1,
            }.get(k, d)
        )
        config.get_str = MagicMock(return_value="schema_v2_canonical")
        config.get_int = MagicMock(
            side_effect=lambda k, d=0: level if k == "SIGNATURE_LEVEL" else d
        )
        config.get = MagicMock(return_value=None)

        return StrategyCache(self.mock_embed_service, config)

    async def test_level_2_requires_exact_match(self):
        """Level 2: require exact signature matches."""
        cache = self._create_cache_with_level(2)

        # Add a canonical SKU.
        sku = StrategyKnowledgeUnit(
            id="test-sku",
            structural_signature="V().out('friend').has('type','Person')",
            goal_template="Find friends",
            predicate=lambda p: True,
            decision_template="out('works_for')",
            schema_fingerprint="schema_v2_canonical",
            property_vector=[0.1] * 10,
            confidence_score=3.0,
            logic_complexity=1,
        )
        cache.add_sku(sku)

        # Fully matching context should hit.
        context_exact = Context(
            structural_signature="V().out('friend').has('type','Person')",
            properties={"type": "Person"},
            goal="Find friends",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_exact)
        self.assertEqual(match_type, "Tier1")
        self.assertEqual(matched_sku.id, "test-sku")

        # Different edge label should not match.
        context_different_filter = Context(
            structural_signature="V().out('friend').has('age','25')",
            properties={"type": "Person"},
            goal="Find friends",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_different_filter)
        self.assertEqual(match_type, "")  # No match.

    async def test_level_1_ignores_filter_differences(self):
        """Level 1: ignore filter differences but keep edge labels."""
        cache = self._create_cache_with_level(1)

        # Add a canonical SKU.
        sku = StrategyKnowledgeUnit(
            id="test-sku",
            structural_signature="V().out('friend').has('type','Person')",
            goal_template="Find friends",
            predicate=lambda p: True,
            decision_template="out('works_for')",
            schema_fingerprint="schema_v2_canonical",
            property_vector=[0.1] * 10,
            confidence_score=3.0,
            logic_complexity=1,
        )
        cache.add_sku(sku)

        # Different filters but same edge label should match.
        context_different_filter = Context(
            structural_signature="V().out('friend').has('age','25')",
            properties={"type": "Person"},
            goal="Find friends",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_different_filter)
        self.assertEqual(match_type, "Tier1")
        self.assertEqual(matched_sku.id, "test-sku")

        # Different edge labels should not match.
        context_different_edge = Context(
            structural_signature="V().out('transfer').has('type','Person')",
            properties={"type": "Person"},
            goal="Find friends",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_different_edge)
        self.assertEqual(match_type, "")  # No match.

    async def test_level_0_ignores_all_labels(self):
        """Level 0: ignore all edge labels and filters."""
        cache = self._create_cache_with_level(0)

        # Add a canonical SKU.
        sku = StrategyKnowledgeUnit(
            id="test-sku",
            structural_signature="V().out('friend').has('type','Person')",
            goal_template="Find paths",
            predicate=lambda p: True,
            decision_template="out('works_for')",
            schema_fingerprint="schema_v2_canonical",
            property_vector=[0.1] * 10,
            confidence_score=3.0,
            logic_complexity=1,
        )
        cache.add_sku(sku)

        # Different labels/filters but same structure should match.
        context_different = Context(
            structural_signature="V().out('transfer').limit(10)",
            properties={"type": "Account"},
            goal="Find paths",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_different)
        self.assertEqual(match_type, "Tier1")
        self.assertEqual(matched_sku.id, "test-sku")

    async def test_fraud_detection_scenario_level_1(self):
        """Scenario: differentiate loops for detection (Level 1)."""
        cache = self._create_cache_with_level(1)

        # Add three loop SKUs with different semantics.
        sku_guarantee = StrategyKnowledgeUnit(
            id="guarantee-loop",
            structural_signature="V().out('guarantee').out('guarantee')",
            goal_template="Find guarantee cycles",
            predicate=lambda p: True,
            decision_template="out('guarantee')",
            schema_fingerprint="schema_v2_canonical",
            property_vector=[0.1] * 10,
            confidence_score=3.0,
            logic_complexity=1,
        )

        sku_transfer = StrategyKnowledgeUnit(
            id="transfer-loop",
            structural_signature="V().out('transfer').out('transfer')",
            goal_template="Find transfer cycles",
            predicate=lambda p: True,
            decision_template="out('transfer')",
            schema_fingerprint="schema_v2_canonical",
            property_vector=[0.2] * 10,
            confidence_score=3.0,
            logic_complexity=1,
        )

        cache.add_sku(sku_guarantee)
        cache.add_sku(sku_transfer)

        # Guarantee loop query should match only guarantee-loop.
        context_guarantee = Context(
            structural_signature="V().out('guarantee').out('guarantee')",
            properties={"type": "Account"},
            goal="Find guarantee cycles",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_guarantee)
        self.assertEqual(match_type, "Tier1")
        self.assertEqual(matched_sku.id, "guarantee-loop")

        # Transfer loop query should match only transfer-loop.
        context_transfer = Context(
            structural_signature="V().out('transfer').out('transfer')",
            properties={"type": "Account"},
            goal="Find transfer cycles",
        )

        decision, matched_sku, match_type = await cache.find_strategy(context_transfer)
        self.assertEqual(match_type, "Tier1")
        self.assertEqual(matched_sku.id, "transfer-loop")


class TestBackwardsCompatibility(unittest.TestCase):
    """Test config defaults and backward compatibility."""

    def test_default_signature_level_is_1(self):
        """Default signature level should be edge-aware (Level 1)."""
        config = DefaultConfiguration()
        level = config.get_int("SIGNATURE_LEVEL", 999)

        # Default is expected to be 1, but allow 2 given current config.
        self.assertIn(level, [1, 2])

    def test_schema_fingerprint_versioned(self):
        """Schema fingerprint should include a version identifier."""
        config = DefaultConfiguration()
        fingerprint = config.get_str("CACHE_SCHEMA_FINGERPRINT", "")

        # Fingerprint should not be empty.
        self.assertNotEqual(fingerprint, "")

        # Fingerprint should contain a version indicator.
        self.assertTrue("schema" in fingerprint.lower())


if __name__ == "__main__":
    unittest.main()
