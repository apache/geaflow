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

"""Integration tests for complete Precheck → Execute → Postcheck lifecycle."""

from unittest.mock import Mock

from core.config import DefaultConfiguration
from harness.simulation.engine import SimulationEngine
from harness.simulation.metrics import MetricsCollector


class MockSKU:
    """Mock SKU for testing."""

    def __init__(self, confidence_score: float = 0.5):
        self.confidence_score = confidence_score
        self.execution_count = 0
        self.success_count = 0


class MockStrategyCache:
    """Mock strategy cache for testing."""

    def __init__(self):
        self.confidence_updates = []

    def update_confidence(self, sku, success):
        """Record confidence updates."""
        self.confidence_updates.append({"sku": sku, "success": success})


class TestLifecycleIntegration:
    """Integration tests for the three-phase execution lifecycle."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DefaultConfiguration()
        self.llm_oracle = Mock()
        self.llm_oracle.config = self.config
        self.strategy_cache = MockStrategyCache()

        # Create mock graph with necessary attributes
        self.mock_graph = Mock()
        self.mock_graph.get_schema.return_value = Mock()

        self.engine = SimulationEngine(
            graph=self.mock_graph,
            strategy_cache=self.strategy_cache,
            llm_oracle=self.llm_oracle,
            verbose=False,
        )

    def test_complete_lifecycle_with_passing_precheck(self):
        """Test full lifecycle when precheck passes."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.5
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add a step with low revisit
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

        sku = MockSKU(confidence_score=0.5)

        # Phase 1: Precheck
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)
        assert should_execute is True
        assert precheck_success is True

        # Phase 2: Execute (simulated)
        execution_result = ["node2", "node3"]

        # Phase 3: Postcheck
        postcheck_result = self.engine.execute_postchecker(
            sku, request_id, metrics, execution_result
        )
        assert postcheck_result is True

        # Verify lifecycle completed successfully
        assert should_execute is True
        assert precheck_success is True
        assert postcheck_result is True

    def test_complete_lifecycle_with_failing_precheck_stop_mode(self):
        """Test full lifecycle when precheck fails in STOP mode."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create high revisit ratio
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

        # Phase 1: Precheck
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)
        assert should_execute is False
        assert precheck_success is False

        # Phase 2 & 3: Should not execute
        # In real code, execution would be skipped and step rolled back

    def test_complete_lifecycle_with_failing_precheck_punish_mode(self):
        """Test full lifecycle when precheck fails in PUNISH mode."""
        self.config.CYCLE_PENALTY = "PUNISH"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create high revisit ratio
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

        # Phase 1: Precheck
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)
        assert should_execute is True  # Continue execution
        assert precheck_success is False  # But signal failure

        # Phase 2: Execute (simulated with penalty)
        execution_result = ["node2"]

        # Phase 3: Postcheck
        postcheck_result = self.engine.execute_postchecker(
            sku, request_id, metrics, execution_result
        )
        assert postcheck_result is True

        # Lifecycle continues but with penalty signal

    def test_rollback_integration_with_precheck_failure(self):
        """Test rollback mechanism integrates with precheck failure."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add steps leading to cycle
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

        initial_step_count = len(metrics.paths[request_id]["steps"])
        assert initial_step_count == 10

        sku = MockSKU(confidence_score=0.5)

        # Precheck fails
        should_execute, _ = self.engine.execute_prechecker(sku, request_id, metrics)

        if not should_execute:
            # Simulate rollback as done in real code
            metrics.rollback_steps(request_id, count=1)

        # Verify step was rolled back
        assert len(metrics.paths[request_id]["steps"]) == initial_step_count - 1

    def test_lifecycle_with_none_sku(self):
        """Test lifecycle with None SKU (new decision)."""
        self.config.CYCLE_PENALTY = "STOP"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Phase 1: Precheck with None SKU
        should_execute, precheck_success = self.engine.execute_prechecker(None, request_id, metrics)
        assert should_execute is True
        assert precheck_success is True

        # Phase 2: Execute (simulated)
        execution_result = ["node2"]

        # Phase 3: Postcheck
        postcheck_result = self.engine.execute_postchecker(
            None, request_id, metrics, execution_result
        )
        assert postcheck_result is True

    def test_lifecycle_confidence_penalty_integration(self):
        """Test confidence penalties integrate correctly with lifecycle."""
        self.config.CYCLE_PENALTY = "PUNISH"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        self.config.MIN_EXECUTION_CONFIDENCE = 0.1
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
                "out('friend')",
            )

        sku = MockSKU(confidence_score=0.5)

        # Precheck fails due to cycle
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should continue but penalize
        assert should_execute is True
        assert precheck_success is False

        # Simulate confidence update (as done in real engine)
        self.strategy_cache.update_confidence(sku, precheck_success)

        # Verify confidence was penalized
        assert len(self.strategy_cache.confidence_updates) == 1
        assert self.strategy_cache.confidence_updates[0]["success"] is False

    def test_lifecycle_multiple_validation_failures(self):
        """Test lifecycle with multiple validation failures."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        self.config.MIN_EXECUTION_CONFIDENCE = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create both cycle and low confidence
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

        sku = MockSKU(confidence_score=0.2)  # Below threshold

        # Precheck should fail on first condition met
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should terminate (STOP mode)
        assert should_execute is False
        assert precheck_success is False

    def test_lifecycle_none_mode_bypasses_all_checks(self):
        """Test NONE mode bypasses entire validation lifecycle."""
        self.config.CYCLE_PENALTY = "NONE"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Create worst-case scenario: high cycles + low confidence
        for i in range(20):
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

        sku = MockSKU(confidence_score=0.01)  # Extremely low

        # Precheck should still pass in NONE mode
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)

        assert should_execute is True
        assert precheck_success is True

    def test_lifecycle_with_empty_path(self):
        """Test lifecycle with newly initialized path (no steps)."""
        self.config.CYCLE_PENALTY = "STOP"
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        sku = MockSKU(confidence_score=0.5)

        # Precheck on empty path
        should_execute, precheck_success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should pass (no cycle possible with empty path)
        assert should_execute is True
        assert precheck_success is True

    def test_lifecycle_preserves_path_state(self):
        """Test lifecycle doesn't modify path state during validation."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.5
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add steps
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

        initial_steps = [step.copy() for step in metrics.paths[request_id]["steps"]]
        sku = MockSKU(confidence_score=0.5)

        # Run precheck
        self.engine.execute_prechecker(sku, request_id, metrics)

        # Run postcheck
        self.engine.execute_postchecker(sku, request_id, metrics, ["node6"])

        # Verify path state unchanged
        assert len(metrics.paths[request_id]["steps"]) == len(initial_steps)
        for i, step in enumerate(metrics.paths[request_id]["steps"]):
            assert step == initial_steps[i]


class TestEdgeCases:
    """Test edge cases in lifecycle integration."""

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

    def test_lifecycle_with_single_step_path(self):
        """Test lifecycle with only one step in path."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.3
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Single step - cannot have cycle
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

        sku = MockSKU(confidence_score=0.5)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Single step should pass (cycle detection requires >= 2 steps)
        assert should_execute is True
        assert success is True

    def test_lifecycle_alternating_pass_fail(self):
        """Test lifecycle with alternating pass/fail pattern."""
        self.config.CYCLE_PENALTY = "PUNISH"
        self.config.CYCLE_DETECTION_THRESHOLD = 0.4
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        results = []

        # Start with low revisit (pass)
        for i in range(3):
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
        results.append(("pass", should_execute, success))

        # Add cycles (fail) - all same node
        for i in range(7):
            metrics.record_path_step(
                request_id,
                3 + i,
                "node1",
                None,
                None,
                None,
                f"sig{3 + i}",
                "goal",
                {},
                "Tier1",
                f"sku{3 + i}",
                "out('friend')",
            )

        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)
        results.append(("fail", should_execute, success))

        # Verify pattern: first passes (0% revisit), second fails (high revisit)
        assert results[0] == ("pass", True, True)
        assert results[1] == ("fail", True, False)  # PUNISH mode continues

    def test_lifecycle_with_zero_confidence(self):
        """Test lifecycle with zero confidence SKU."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.MIN_EXECUTION_CONFIDENCE = 0.1
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

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

        sku = MockSKU(confidence_score=0.0)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should fail due to confidence < 0.1
        assert should_execute is False
        assert success is False

    def test_lifecycle_with_perfect_confidence(self):
        """Test lifecycle with perfect confidence SKU."""
        self.config.CYCLE_PENALTY = "STOP"
        self.config.MIN_EXECUTION_CONFIDENCE = 0.9
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

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

        sku = MockSKU(confidence_score=1.0)
        should_execute, success = self.engine.execute_prechecker(sku, request_id, metrics)

        # Should pass all checks
        assert should_execute is True
        assert success is True
