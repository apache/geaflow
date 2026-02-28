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

"""Unit tests for MetricsCollector class."""

from harness.simulation.metrics import MetricsCollector


class TestMetricsCollector:
    """Test MetricsCollector functionality."""

    def test_initialize_path(self):
        """Test path initialization creates correct structure."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {"key": "value"}, "goal", "rubric")

        assert request_id in metrics.paths
        path = metrics.paths[request_id]
        assert path["start_node"] == "node1"
        assert path["start_node_props"] == {"key": "value"}
        assert path["goal"] == "goal"
        assert path["rubric"] == "rubric"
        assert path["steps"] == []

    def test_record_path_step(self):
        """Test recording path steps stores correct information."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        metrics.record_path_step(
            request_id=request_id,
            tick=0,
            node_id="node1",
            parent_node=None,
            parent_step_index=None,
            edge_label=None,
            structural_signature="V().out('knows')",
            goal="goal",
            properties={"name": "Alice"},
            match_type="Tier1",
            sku_id="sku1",
            decision="out('knows')",
        )

        steps = metrics.paths[request_id]["steps"]
        assert len(steps) == 1
        assert steps[0]["node"] == "node1"
        assert steps[0]["s"] == "V().out('knows')"
        assert steps[0]["match_type"] == "Tier1"


class TestRollbackSteps:
    """Test rollback_steps functionality."""

    def test_single_step_rollback(self):
        """Test rolling back a single step."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        metrics.record_path_step(
            request_id, 0, "node1", None, None, None, "sig", "goal", {}, "Tier1", "sku1", "decision"
        )
        assert len(metrics.paths[request_id]["steps"]) == 1
        assert metrics.rollback_steps(request_id, count=1) is True
        assert len(metrics.paths[request_id]["steps"]) == 0

    def test_multi_step_rollback(self):
        """Test rolling back multiple steps at once."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add 3 steps
        metrics.record_path_step(
            request_id, 0, "node1", None, None, None, "sig1", "goal", {}, "Tier1", "sku1", "d1"
        )
        metrics.record_path_step(
            request_id, 1, "node2", None, None, None, "sig2", "goal", {}, "Tier1", "sku2", "d2"
        )
        metrics.record_path_step(
            request_id, 2, "node3", None, None, None, "sig3", "goal", {}, "Tier1", "sku3", "d3"
        )
        assert len(metrics.paths[request_id]["steps"]) == 3

        # Rollback 2 steps
        assert metrics.rollback_steps(request_id, count=2) is True
        assert len(metrics.paths[request_id]["steps"]) == 1
        # Verify remaining step is the first one
        assert metrics.paths[request_id]["steps"][0]["node"] == "node1"

    def test_rollback_insufficient_steps(self):
        """Test rollback fails when insufficient steps available."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        metrics.record_path_step(
            request_id, 0, "node1", None, None, None, "sig", "goal", {}, "Tier1", "sku1", "d1"
        )

        # Try to rollback 2 steps when only 1 exists
        assert metrics.rollback_steps(request_id, count=2) is False
        # Path should be unchanged
        assert len(metrics.paths[request_id]["steps"]) == 1

    def test_rollback_empty_path(self):
        """Test rollback on empty path."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Path is empty, rollback should fail
        assert metrics.rollback_steps(request_id, count=1) is False
        assert len(metrics.paths[request_id]["steps"]) == 0

    def test_rollback_zero_count(self):
        """Test rollback with count=0 always succeeds."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        metrics.record_path_step(
            request_id, 0, "node1", None, None, None, "sig", "goal", {}, "Tier1", "sku1", "d1"
        )

        # Rollback 0 steps should succeed but not change anything
        assert metrics.rollback_steps(request_id, count=0) is True
        assert len(metrics.paths[request_id]["steps"]) == 1

    def test_rollback_nonexistent_request(self):
        """Test rollback on non-existent request_id."""
        metrics = MetricsCollector()

        # Request ID 999 doesn't exist
        assert metrics.rollback_steps(999, count=1) is False

    def test_rollback_multiple_times(self):
        """Test successive rollbacks work correctly."""
        metrics = MetricsCollector()
        request_id = metrics.initialize_path(0, "node1", {}, "goal", "rubric")

        # Add 5 steps
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
                f"d{i}",
            )
        assert len(metrics.paths[request_id]["steps"]) == 5

        # Rollback 2, then 1, then 2 more
        assert metrics.rollback_steps(request_id, count=2) is True
        assert len(metrics.paths[request_id]["steps"]) == 3

        assert metrics.rollback_steps(request_id, count=1) is True
        assert len(metrics.paths[request_id]["steps"]) == 2

        assert metrics.rollback_steps(request_id, count=2) is True
        assert len(metrics.paths[request_id]["steps"]) == 0

    def test_rollback_preserves_other_paths(self):
        """Test rollback only affects the specified path."""
        metrics = MetricsCollector()
        req1 = metrics.initialize_path(0, "node1", {}, "goal1", "rubric1")
        req2 = metrics.initialize_path(1, "node2", {}, "goal2", "rubric2")

        # Add steps to both paths
        metrics.record_path_step(req1, 0, "n1", None, None, None, "s1", "g1", {}, "T1", "sk1", "d1")
        metrics.record_path_step(req1, 1, "n2", None, None, None, "s2", "g1", {}, "T1", "sk2", "d2")
        metrics.record_path_step(req2, 0, "n3", None, None, None, "s3", "g2", {}, "T1", "sk3", "d3")

        # Rollback path 1
        assert metrics.rollback_steps(req1, count=1) is True

        # Path 1 should have 1 step, path 2 should be unchanged
        assert len(metrics.paths[req1]["steps"]) == 1
        assert len(metrics.paths[req2]["steps"]) == 1
        assert metrics.paths[req2]["steps"][0]["node"] == "n3"
