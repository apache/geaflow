"""
This module contains unit tests for the CASTS reasoning engine core logic,
focused on the correctness of `InMemoryGraphSchema` and `GremlinStateMachine`.

All tests are designed to be fully independent of any external LLM calls,
ensuring that graph traversal and state management logic is correct,
deterministic, and robust.

---

### Test strategy and case design notes

1. **`TestGraphSchema`**:
   - **Goal**: Verify that schema extraction correctly identifies and separates
     outgoing and incoming edge labels per node.
   - **Method**: Build a mock graph in `setUp`, then assert that
     `get_valid_outgoing_edge_labels` and `get_valid_incoming_edge_labels`
     return expected labels for different nodes.
   - **Key cases**:
     - **Node `A`**: Has both outgoing (`friend`, `works_for`) and incoming
       (`friend`, `employs`) edges to test mixed behavior.
     - **Node `B`**: Focus on outgoing labels (`friend` to `A`).
     - **Node `D`**: Has only incoming edges (`partner` from `C`) and no outgoing
       edges, ensuring `get_valid_outgoing_edge_labels` returns an empty list and
       prevents fallback to global labels.
     - **Incoming/outgoing separation**: Ensure outgoing and incoming label lists
       are strictly separated and correct.

2. **`TestGremlinStateMachine`**:
   - **Goal**: Verify integration with `GraphSchema`, ensure valid Gremlin step
     options are generated for the current node context, and validate state
     transitions.
   - **Method**: Build a mock schema and call `get_state_and_options` with
     different `structural_signature` values and node IDs.
   - **Key cases**:
     - **Schema integration (`test_vertex_state_options`)**:
       - **Idea**: Check concrete, schema-derived steps rather than generic
         `out('label')`.
       - **Verify**: For node `A` (outgoing `friend` and `knows`), options must
         include `out('friend')` and `out('knows')`.
     - **Directionality (`test_vertex_state_options`)**:
       - **Idea**: Ensure `in`/`out` steps are generated from the correct edge
         directions.
       - **Verify**: For node `A`, `in('friend')` must appear (incoming from `B`);
         `in('knows')` must not appear.
     - **Empty labels (`test_empty_labels`)**:
       - **Idea**: Do not generate steps for missing labels on a direction.
       - **Verify**: Node `B` has no outgoing `knows`, so `out('knows')` must be
         absent while `in('knows')` and `both('knows')` remain valid.
     - **State transitions (`test_state_transitions`)**:
       - **Idea**: Ensure Gremlin transitions follow V -> E -> V.
       - **Verify**: `V().outE(...)` yields `E`; `V().outE(...).inV()` returns to `V`.
     - **Invalid transitions (`test_invalid_transition`)**:
       - **Idea**: Enforce strict syntax.
       - **Verify**: `V().outV()` must lead to `END` with no options.
"""
import unittest

from casts.core.gremlin_state import GremlinStateMachine
from casts.core.schema import InMemoryGraphSchema


class TestGraphSchema(unittest.TestCase):
    """Test cases for InMemoryGraphSchema class."""

    def setUp(self):
        """Set up a mock graph schema for testing."""
        nodes = {
            'A': {'id': 'A', 'type': 'Person'},
            'B': {'id': 'B', 'type': 'Person'},
            'C': {'id': 'C', 'type': 'Company'},
            'D': {'id': 'D', 'type': 'Person'},  # Node with only incoming edges
        }
        edges = {
            'A': [
                {'label': 'friend', 'target': 'B'},
                {'label': 'works_for', 'target': 'C'},
            ],
            'B': [
                {'label': 'friend', 'target': 'A'},
            ],
            'C': [
                {'label': 'employs', 'target': 'A'},
                {'label': 'partner', 'target': 'D'},
            ],
        }
        self.schema = InMemoryGraphSchema(nodes, edges)

    def test_get_valid_outgoing_edge_labels(self):
        """Test that get_valid_outgoing_edge_labels returns correct outgoing labels."""
        self.assertCountEqual(
            self.schema.get_valid_outgoing_edge_labels('A'), ['friend', 'works_for']
        )
        self.assertCountEqual(
            self.schema.get_valid_outgoing_edge_labels('B'), ['friend']
        )
        self.assertCountEqual(
            self.schema.get_valid_outgoing_edge_labels('C'), ['employs', 'partner']
        )

    def test_get_valid_outgoing_edge_labels_no_outgoing(self):
        """Test get_valid_outgoing_edge_labels returns empty list with no outgoing edges."""
        self.assertEqual(self.schema.get_valid_outgoing_edge_labels('D'), [])

    def test_get_valid_incoming_edge_labels(self):
        """Test that get_valid_incoming_edge_labels returns correct incoming labels."""
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('A'), ['friend', 'employs']
        )
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('B'), ['friend']
        )
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('C'), ['works_for']
        )
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('D'), ['partner']
        )

    def test_get_valid_incoming_edge_labels_no_incoming(self):
        """Test get_valid_incoming_edge_labels returns empty list with no incoming edges."""
        # In our test setup, node C has no incoming edges from other defined nodes
        # in this context, but the logic should handle it gracefully. This test
        # relies on the setUp structure.
        pass  # Placeholder, current structure has all nodes with incoming edges.


class TestGremlinStateMachine(unittest.TestCase):

    def setUp(self):
        """Set up a mock graph schema for testing the state machine."""
        nodes = {
            'A': {'id': 'A', 'type': 'Person'},
            'B': {'id': 'B', 'type': 'Person'},
        }
        edges = {
            'A': [
                {'label': 'friend', 'target': 'B'},
                {'label': 'knows', 'target': 'B'},
            ],
            'B': [
                {'label': 'friend', 'target': 'A'},
            ],
        }
        self.schema = InMemoryGraphSchema(nodes, edges)

    def test_vertex_state_options(self):
        """Test that the state machine generates correct, concrete options from a vertex state."""
        state, options = GremlinStateMachine.get_state_and_options("V()", self.schema, 'A')
        self.assertEqual(state, "V")

        # Check for concrete 'out' steps
        self.assertIn("out('friend')", options)
        self.assertIn("out('knows')", options)

        # Check for concrete 'in' steps (node A has one incoming 'friend' edge from B)
        self.assertIn("in('friend')", options)
        self.assertNotIn("in('knows')", options)

        # Check for concrete 'both' steps
        self.assertIn("both('friend')", options)
        self.assertIn("both('knows')", options)

        # Check for non-label steps
        self.assertIn("has('prop','value')", options)
        self.assertIn("stop", options)

    def test_empty_labels(self):
        """Test that no label-based steps are generated if there are no corresponding edges."""
        state, options = GremlinStateMachine.get_state_and_options("V()", self.schema, 'B')
        self.assertEqual(state, "V")
        # Node B has an outgoing 'friend' edge and incoming 'friend' and 'knows' edges.
        # It has no outgoing 'knows' edge.
        self.assertNotIn("out('knows')", options)
        self.assertIn("in('knows')", options)
        self.assertIn("both('knows')", options)

    def test_state_transitions(self):
        """Test that the state machine correctly transitions between states."""
        # V -> E
        state, _ = GremlinStateMachine.get_state_and_options(
            "V().outE('friend')", self.schema, 'B'
        )
        self.assertEqual(state, "E")

        # V -> E -> V
        state, _ = GremlinStateMachine.get_state_and_options(
            "V().outE('friend').inV()", self.schema, 'A'
        )
        self.assertEqual(state, "V")

    def test_invalid_transition(self):
        """Test that an invalid sequence of steps leads to the END state."""
        state, options = GremlinStateMachine.get_state_and_options("V().outV()", self.schema, 'A')
        self.assertEqual(state, "END")
        self.assertEqual(options, [])

    def test_generic_vertex_steps(self):
        """Test that generic (non-label) steps are available at a vertex state."""
        _, options = GremlinStateMachine.get_state_and_options("V()", self.schema, 'A')
        self.assertIn("has('prop','value')", options)
        self.assertIn("dedup()", options)
        self.assertIn("order().by('prop')", options)
        self.assertIn("limit(n)", options)
        self.assertIn("values('prop')", options)

    def test_edge_to_vertex_steps(self):
        """Test that edge-to-vertex steps are available at an edge state."""
        # Transition to an edge state first
        state, options = GremlinStateMachine.get_state_and_options(
            "V().outE('friend')", self.schema, 'A'
        )
        self.assertEqual(state, "E")

        # Now check for edge-specific steps
        self.assertIn("inV()", options)
        self.assertIn("outV()", options)
        self.assertIn("otherV()", options)

    def test_order_by_modifier_keeps_state(self):
        """Test that order().by() modifier does not invalidate state."""
        state, options = GremlinStateMachine.get_state_and_options(
            "V().order().by('prop')", self.schema, "A"
        )
        self.assertEqual(state, "V")
        self.assertIn("stop", options)
