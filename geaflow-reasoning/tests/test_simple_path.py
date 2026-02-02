"""Unit tests for simplePath() functionality."""

import pytest

from casts.core.gremlin_state import GREMLIN_STEP_STATE_MACHINE
from casts.services.llm_oracle import LLMOracle


class TestGremlinStateMachine:
    """Test simplePath() integration in GremlinStateMachine."""

    def test_simple_path_in_vertex_options(self):
        """Test that simplePath() is available as an option in Vertex state."""
        vertex_options = GREMLIN_STEP_STATE_MACHINE["V"]["options"]
        assert "simplePath()" in vertex_options

    def test_simple_path_in_edge_options(self):
        """Test that simplePath() is available as an option in Edge state."""
        edge_options = GREMLIN_STEP_STATE_MACHINE["E"]["options"]
        assert "simplePath()" in edge_options

    def test_simple_path_in_property_options(self):
        """Test that simplePath() is available as an option in Property state."""
        property_options = GREMLIN_STEP_STATE_MACHINE["P"]["options"]
        assert "simplePath()" in property_options

    def test_simple_path_vertex_transition(self):
        """Test that simplePath() from Vertex state stays in Vertex state."""
        transitions = GREMLIN_STEP_STATE_MACHINE["V"]["transitions"]
        assert transitions["simplePath"] == "V"

    def test_simple_path_edge_transition(self):
        """Test that simplePath() from Edge state stays in Edge state."""
        transitions = GREMLIN_STEP_STATE_MACHINE["E"]["transitions"]
        assert transitions["simplePath"] == "E"

    def test_simple_path_property_transition(self):
        """Test that simplePath() from Property state stays in Property state."""
        transitions = GREMLIN_STEP_STATE_MACHINE["P"]["transitions"]
        assert transitions["simplePath"] == "P"


class TestHistoryExtraction:
    """Test decision history extraction from LLM Oracle."""

    def test_empty_signature(self):
        """Test history extraction from empty signature."""
        result = LLMOracle._extract_recent_decisions("", depth=3)
        assert result == []

    def test_v_only_signature(self):
        """Test history extraction from V() only signature."""
        result = LLMOracle._extract_recent_decisions("V()", depth=3)
        assert result == []

    def test_single_decision(self):
        """Test history extraction with single decision."""
        signature = "V().out('friend')"
        result = LLMOracle._extract_recent_decisions(signature, depth=3)
        assert result == ["out('friend')"]

    def test_multiple_decisions(self):
        """Test history extraction with multiple decisions."""
        signature = "V().out('friend').has('type','Person').out('supplier')"
        result = LLMOracle._extract_recent_decisions(signature, depth=3)
        assert result == ["out('friend')", "has('type','Person')", "out('supplier')"]

    def test_with_simple_path(self):
        """Test history extraction with simplePath() in signature."""
        signature = "V().out('friend').simplePath().out('supplier')"
        result = LLMOracle._extract_recent_decisions(signature, depth=3)
        assert result == ["out('friend')", "simplePath()", "out('supplier')"]

    def test_depth_limit(self):
        """Test that history extraction respects depth limit."""
        signature = "V().out('a').out('b').out('c').out('d').out('e')"
        result = LLMOracle._extract_recent_decisions(signature, depth=3)
        assert len(result) == 3
        assert result == ["out('c')", "out('d')", "out('e')"]

    def test_no_arguments_step(self):
        """Test extraction of steps with no arguments."""
        signature = "V().out('friend').dedup().simplePath()"
        result = LLMOracle._extract_recent_decisions(signature, depth=5)
        assert result == ["out('friend')", "dedup()", "simplePath()"]


@pytest.mark.asyncio
class TestSimplePathExecution:
    """Test simplePath() execution in TraversalExecutor."""

    @pytest.fixture
    def mock_graph(self):
        """Create a simple mock graph for testing."""
        # Create a simple graph: A -> B -> C -> A (triangle)
        class MockGraph:
            def __init__(self):
                self.nodes = {
                    "A": {"id": "A", "type": "Node"},
                    "B": {"id": "B", "type": "Node"},
                    "C": {"id": "C", "type": "Node"},
                }
                self.edges = {
                    "A": [{"label": "friend", "target": "B"}],
                    "B": [{"label": "friend", "target": "C"}],
                    "C": [{"label": "friend", "target": "A"}],
                }

        return MockGraph()

    @pytest.fixture
    def mock_schema(self):
        """Create a mock schema."""
        class MockSchema:
            def get_valid_outgoing_edge_labels(self, node_id):
                return ["friend"]

            def get_valid_incoming_edge_labels(self, node_id):
                return ["friend"]

        return MockSchema()

    async def test_simple_path_step_execution(self, mock_graph, mock_schema):
        """Test that simplePath() step passes through current node."""
        from casts.simulation.executor import TraversalExecutor

        executor = TraversalExecutor(mock_graph, mock_schema)

        # Execute simplePath() on node A
        result = await executor.execute_decision(
            current_node_id="A",
            decision="simplePath()",
            current_signature="V()",
            request_id=1,
        )

        # simplePath() should pass through the current node
        assert len(result) == 1
        assert result[0][0] == "A"  # Same node ID
        assert result[0][1] == "V().simplePath()"  # Updated signature

    async def test_simple_path_filtering(self, mock_graph, mock_schema):
        """Test that simplePath filters out visited nodes."""
        from casts.simulation.executor import TraversalExecutor

        executor = TraversalExecutor(mock_graph, mock_schema)

        # First, traverse A -> B
        result1 = await executor.execute_decision(
            current_node_id="A",
            decision="out('friend')",
            current_signature="V().simplePath()",
            request_id=1,
        )
        assert len(result1) == 1
        assert result1[0][0] == "B"

        # Then traverse B -> C
        result2 = await executor.execute_decision(
            current_node_id="B",
            decision="out('friend')",
            current_signature="V().simplePath().out('friend')",
            request_id=1,
        )
        assert len(result2) == 1
        assert result2[0][0] == "C"

        # Finally, try to traverse C -> A (should be filtered out)
        result3 = await executor.execute_decision(
            current_node_id="C",
            decision="out('friend')",
            current_signature="V().simplePath().out('friend').out('friend')",
            request_id=1,
        )
        # Should be empty because A was already visited
        assert len(result3) == 0

    async def test_without_simple_path_allows_cycles(self, mock_graph, mock_schema):
        """Test that without simplePath(), cycles are allowed."""
        from casts.simulation.executor import TraversalExecutor

        executor = TraversalExecutor(mock_graph, mock_schema)

        # Traverse A -> B without simplePath
        result1 = await executor.execute_decision(
            current_node_id="A",
            decision="out('friend')",
            current_signature="V()",
            request_id=2,
        )
        assert len(result1) == 1
        assert result1[0][0] == "B"

        # Traverse B -> C
        result2 = await executor.execute_decision(
            current_node_id="B",
            decision="out('friend')",
            current_signature="V().out('friend')",
            request_id=2,
        )
        assert len(result2) == 1
        assert result2[0][0] == "C"

        # Traverse C -> A (should work because simplePath is not enabled)
        result3 = await executor.execute_decision(
            current_node_id="C",
            decision="out('friend')",
            current_signature="V().out('friend').out('friend')",
            request_id=2,
        )
        assert len(result3) == 1
        assert result3[0][0] == "A"  # Cycle is allowed

    async def test_simple_path_allows_filter_steps(self, mock_graph, mock_schema):
        """Test that simplePath does not block non-traversal filter steps."""
        from casts.simulation.executor import TraversalExecutor

        executor = TraversalExecutor(mock_graph, mock_schema)

        await executor.execute_decision(
            current_node_id="A",
            decision="simplePath()",
            current_signature="V()",
            request_id=4,
        )

        result = await executor.execute_decision(
            current_node_id="A",
            decision="has('type','Node')",
            current_signature="V().simplePath()",
            request_id=4,
        )

        assert len(result) == 1
        assert result[0][0] == "A"

    async def test_clear_path_history(self, mock_graph, mock_schema):
        """Test that clear_path_history properly cleans up."""
        from casts.simulation.executor import TraversalExecutor

        executor = TraversalExecutor(mock_graph, mock_schema)

        # Execute with simplePath to populate history
        await executor.execute_decision(
            current_node_id="A",
            decision="out('friend')",
            current_signature="V().simplePath()",
            request_id=3,
        )

        # Verify history exists
        assert 3 in executor._path_history
        assert "A" in executor._path_history[3]

        # Clear history
        executor.clear_path_history(3)

        # Verify history is cleared
        assert 3 not in executor._path_history
