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

"""Unit tests for Execution Lifecycle (Precheck → Execute → Postcheck)."""

from unittest.mock import Mock

from core.config import DefaultConfiguration
from harness.simulation.engine import SimulationEngine
from harness.simulation.metrics import MetricsCollector


class MockSKU:
    """Mock SKU for testing."""

    def __init__(self, confidence_score: float = 0.5):
        self.confidence_score = confidence_score


class TestExecutePrechecker:
    """Test execute_prechecker() validation logic."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DefaultConfiguration()
        self.llm_oracle = Mock()
        self.llm_oracle.config = self.config

        # Create mock graph with necessary attributes
        self.mock_graph = Mock()
        self.mock_graph.get_schema.return_value = Mock()

        self.engine = SimulationEngine(
            graph=self.mock_graph, strategy_cache=Mock(), llm_oracle=self.llm_oracle, verbose=False
        )

    def test_none_mode_skips_all_validation(self):
        """Test CYCLE_PENALTY=NONE skips all validation."""
        self.config.CYCLE_PENALTY = "NONE"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add steps that would normally fail cycle detection
        for i in range(10):
            metrics.record_path_step(
                request_id,
                i,
                "node1",
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should always return (True, True) in NONE mode
        assert should_execute is True
        assert success is True

    def test_punish_mode_continues_with_penalty(self):
        """Test CYCLE_PENALTY=PUNISH continues execution but penalizes."""
        self.config.CYCLE_PENALTY = "PUNISH"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create high revisit ratio: 10 steps, 2 unique nodes = 80% revisit
        for i in range(10):
            node_id = "node1" if i % 2 == 0 else "node2"
            metrics.record_path_step(
                request_id,
                i,
                node_id,
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should continue but signal failure for penalty
        assert should_execute is True
        assert success is False

    def test_stop_mode_terminates_path(self):
        """Test CYCLE_PENALTY=STOP terminates path on cycle detection."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create high revisit ratio: 10 steps, 2 unique nodes = 80% revisit
        for i in range(10):
            node_id = "node1" if i % 2 == 0 else "node2"
            metrics.record_path_step(
                request_id,
                i,
                node_id,
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should terminate and signal failure
        assert should_execute is False
        assert success is False

    def test_low_revisit_ratio_passes(self):
        """Test low revisit ratio passes cycle detection."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.5
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create low revisit ratio: 5 unique nodes out of 5 steps = 0% revisit
        for i in range(5):
            metrics.record_path_step(
                request_id,
                i,
                f"node{i}",
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should pass all checks (0% revisit < 50% threshold)
        assert should_execute is True
        assert success is True

    def test_simple_path_skips_cycle_detection(self):
        """Test simplePath() skips cycle detection penalty."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.1
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        for i in range(5):
            metrics.record_path_step(
                request_id,
                i,
                "node1",
                None,
                None,
                None,
                "V().simplePath()",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        assert should_execute is True
        assert success is True

    def test_confidence_threshold_stop_mode(self):
        """Test MIN_EXECUTION_CONFIDENCE check in STOP mode."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.MIN_EXECUTION_CONFIDENCE = 0.2
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add a single step to avoid cycle detection
        metrics.record_path_step(
            request_id,
            0,
            "node1",
            None,
            None,
            None,
            "sig",
            "goal",
            {},
            "Tier1",
            "sku1",
            "out('friend')",
        )

        # SKU with confidence below threshold
        sku = MockSKU(confidence_score=0.1)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should terminate due to low confidence
        assert should_execute is False
        assert success is False

    def test_confidence_threshold_punish_mode(self):
        """Test MIN_EXECUTION_CONFIDENCE check in PUNISH mode."""
        self.config.CYCLE_PENALTY = "PUNISH"
        self.config.MIN_EXECUTION_CONFIDENCE = 0.2
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add a single step to avoid cycle detection
        metrics.record_path_step(
            request_id,
            0,
            "node1",
            None,
            None,
            None,
            "sig",
            "goal",
            {},
            "Tier1",
            "sku1",
            "out('friend')",
        )

        # SKU with confidence below threshold
        sku = MockSKU(confidence_score=0.1)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should continue but penalize
        assert should_execute is True
        assert success is False

    def test_no_sku_passes_validation(self):
        """Test None SKU passes validation (new SKUs)."""
        self.config.CYCLE_PENALTY = "STOP"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        should_execute, success = self.engine.execute_prechecker(None, request_id, metrics)

        # None SKU should always pass
        assert should_execute is True
        assert success is True

    def test_nonexistent_request_id_passes(self):
        """Test non-existent request_id passes validation."""
        self.config.CYCLE_PENALTY = "STOP"
        metrics = MetricsCollector()
        sku = MockSKU(confidence_score=0.5)

        should_execute, success = self.engine.execute_prechecker(
            sku,
            999,
            metrics,  # Non-existent request ID
        )

        # Should pass since path doesn't exist
        assert should_execute is True
        assert success is True

    def test_cycle_detection_threshold_boundary(self):
        """Test cycle detection at exact threshold boundary."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.5  # 50%
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create exactly 50% revisit: 2 steps, 1 unique node
        metrics.record_path_step(
            request_id,
            0,
            "node1",
            None,
            None,
            None,
            "sig1",
            "goal",
            {},
            "Tier1",
            "sku1",
            "out('friend')",
        )
        metrics.record_path_step(
            request_id,
            1,
            "node1",
            None,
            None,
            None,
            "sig2",
            "goal",
            {},
            "Tier1",
            "sku2",
            "out('friend')",
        )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should pass at exactly threshold (not greater than)
        assert should_execute is True
        assert success is True

    def test_cycle_detection_just_above_threshold(self):
        """Test cycle detection just above threshold."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create 40% revisit: 5 steps, 3 unique nodes
        # Revisit ratio = 1 - (3/5) = 0.4 > 0.3
        for i in range(5):
            node_id = f"node{i % 3}"  # Cycles through 3 nodes
            metrics.record_path_step(
                request_id,
                i,
                node_id,
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should fail cycle detection
        assert should_execute is False
        assert success is False


class TestExecutePostchecker:
    """Test execute_postchecker() placeholder functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DefaultConfiguration()
        self.llm_oracle = Mock()
        self.llm_oracle.config = self.config

        # Create mock graph with necessary attributes
        self.mock_graph = Mock()
        self.mock_graph.get_schema.return_value = Mock()

        self.engine = SimulationEngine(
            graph=self.mock_graph, strategy_cache=Mock(), llm_oracle=self.llm_oracle, verbose=False
        )

    def test_postchecker_always_returns_true(self):
        """Test postchecker currently always returns True."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")
        sku = MockSKU()
        execution_result = ["node2", "node3"]

        result = self.engine.execute_postchecker(sku, request_id, metrics, execution_result)

        assert result is True

    def test_postchecker_with_none_sku(self):
        """Test postchecker with None SKU."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")
        execution_result = []

        result = self.engine.execute_postchecker(None, request_id, metrics, execution_result)

        assert result is True

    def test_postchecker_with_empty_result(self):
        """Test postchecker with empty execution result."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")
        sku = MockSKU()

        result = self.engine.execute_postchecker(sku, request_id, metrics, [])

        assert result is True


class TestCyclePenaltyModes:
    """Test CYCLE_PENALTY configuration modes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DefaultConfiguration()
        self.llm_oracle = Mock()
        self.llm_oracle.config = self.config

        # Create mock graph with necessary attributes
        self.mock_graph = Mock()
        self.mock_graph.get_schema.return_value = Mock()

        self.engine = SimulationEngine(
            graph=self.mock_graph, strategy_cache=Mock(), llm_oracle=self.llm_oracle, verbose=False
        )

    def test_mode_none_case_insensitive(self):
        """Test CYCLE_PENALTY=none (lowercase) works."""
        self.config.CYCLE_PENALTY = "none"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add cyclic steps
        for i in range(5):
            metrics.record_path_step(
                request_id,
                i,
                "node1",
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                f"d{i}",
            )

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # NONE mode should skip validation even with lowercase
        assert should_execute is True
        assert success is True

    def test_mode_punish_case_variants(self):
        """Test CYCLE_PENALTY mode handles case variants."""
        test_cases = ["PUNISH", "punish", "Punish"]

        for mode in test_cases:
            self.config.CYCLE_PENALTY = mode
            self.config.CYCLE_DETECTION_THRESHOLD = 0.3
            metrics = MetricsCollector()
            request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

            # Create high revisit
            for i in range(10):
                metrics.record_path_step(
                    request_id,
                    i,
                    "node1",
                    None,
                    None,
                    None,
                    f"sig{i}",
                    "goal",
                    {},
                    "Tier1",
                    f"sku{i}",
                    "out('friend')",
                )

            sku = MockSKU(confidence_score=0.5)
            should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

            # All variants should work consistently
            assert should_execute is True
            assert success is False


class TestConfigurationParameters:
    """Test configuration parameter handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DefaultConfiguration()
        self.llm_oracle = Mock()
        self.llm_oracle.config = self.config

        # Create mock graph with necessary attributes
        self.mock_graph = Mock()
        self.mock_graph.get_schema.return_value = Mock()

        self.engine = SimulationEngine(
            graph=self.mock_graph, strategy_cache=Mock(), llm_oracle=self.llm_oracle, verbose=False
        )

    def test_cycle_detection_threshold_default(self):
        """Test CYCLE_DETECTION_THRESHOLD has correct default."""
        assert self.config.CYCLE_DETECTION_THRESHOLD == 0.7

    def test_min_execution_confidence_default(self):
        """Test MIN_EXECUTION_CONFIDENCE has correct default."""
        assert self.config.MIN_EXECUTION_CONFIDENCE == 0.1

    def test_cycle_penalty_default(self):
        """Test CYCLE_PENALTY has correct default."""
        assert self.config.CYCLE_PENALTY == "STOP"

    def test_custom_threshold_values(self):
        """Test custom threshold values are respected."""
        self.config.CYCLE_DETECTION_THRESHOLD = 0.8
        self.config.MIN_EXECUTION_CONFIDENCE = 0.5
        self.config.CYCLE_PENALTY = "PUNISH"

        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create 85% revisit (above 0.8 threshold)
        for i in range(20):
            node_id = f"node{i % 3}"
            metrics.record_path_step(
                request_id,
                i,
                node_id,
                None,
                None,
                None,
                f"sig{i}",
                "goal",
                {},
                "Tier1",
                f"sku{i}",
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.6)  # Above 0.5 min confidence
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should fail cycle detection but pass confidence check
        assert should_execute is True  # PUNISH mode continues
        assert success is False  # But signals failure
