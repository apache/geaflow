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
This module contains unit tests for the CASTS reasoning engine core logic,
focused on the correctness of `InMemoryGraphSchema` and `GremlinStateMachine`.

All tests are designed to be fully independent of any external LLM calls,
ensuring that graph traversal and state management logic is correct,
deterministic, and robust.

---

### Test strategy and case design notes

1. **`TestGraphSchema`**:
   - **Goal**: Verify that schema extraction correctly identifies and separates
     outgoing and incoming edge labels per node type.
   - **Method**: Build a mock graph in `setUp`, then assert that
     `get_valid_outgoing_edge_labels` and `get_valid_incoming_edge_labels`
     return expected labels for different node types.
   - **Key cases**:
     - **Type `Person`**: Aggregates outgoing (`friend`, `works_for`) and incoming
       (`friend`, `employs`, `partner`) edges from all Person nodes.
     - **Type `Company`**: Captures outgoing labels from the company node and
       incoming labels from edges into the company node.
     - **Incoming/outgoing separation**: Ensure outgoing and incoming label lists
       are strictly separated and correct per type.

2. **`TestGremlinStateMachine`**:
   - **Goal**: Verify integration with `GraphSchema`, ensure valid Gremlin step
     options are generated for the current node context, and validate state
     transitions.
   - **Method**: Build a mock schema and call `get_state_and_options` with
     different `structural_signature` values and node types.
   - **Key cases**:
     - **Schema integration (`test_vertex_state_options`)**:
       - **Idea**: Check concrete, schema-derived steps rather than generic
         `out('label')`.
       - **Verify**: For type `Person` (outgoing `friend` and `knows`), options must
         include `out('friend')` and `out('knows')`.
     - **Directionality (`test_vertex_state_options`)**:
       - **Idea**: Ensure `in`/`out` steps are generated from the correct edge
         directions.
       - **Verify**: For type `Person`, `in('friend')` must appear (incoming from `B`);
         `in('knows')` must appear if any Person receives a `knows` edge.
     - **Empty labels (`test_empty_labels`)**:
       - **Idea**: Do not generate steps for missing labels on a direction.
       - **Verify**: Type `Company` has outgoing labels but no incoming labels in
         this mock schema, so `in('employs')` must be absent while
         `out('employs')` and `both('employs')` remain valid.
     - **State transitions (`test_state_transitions`)**:
       - **Idea**: Ensure Gremlin transitions follow V -> E -> V.
       - **Verify**: `V().outE(...)` yields `E`; `V().outE(...).inV()` returns to `V`.
     - **Invalid transitions (`test_invalid_transition`)**:
       - **Idea**: Enforce strict syntax.
       - **Verify**: `V().outV()` must lead to `END` with no options.
"""
import unittest

from core.gremlin_state import GremlinStateMachine
from core.schema import InMemoryGraphSchema


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
            self.schema.get_valid_outgoing_edge_labels('Person'), ['friend', 'works_for']
        )
        self.assertCountEqual(
            self.schema.get_valid_outgoing_edge_labels('Company'), ['employs', 'partner']
        )

    def test_get_valid_outgoing_edge_labels_no_outgoing(self):
        """Test get_valid_outgoing_edge_labels returns empty list with no outgoing edges."""
        self.assertEqual(self.schema.get_valid_outgoing_edge_labels('Unknown'), [])

    def test_get_valid_incoming_edge_labels(self):
        """Test that get_valid_incoming_edge_labels returns correct incoming labels."""
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('Person'),
            ['employs', 'friend', 'partner'],
        )
        self.assertCountEqual(
            self.schema.get_valid_incoming_edge_labels('Company'), ['works_for']
        )

    def test_get_valid_incoming_edge_labels_no_incoming(self):
        """Test get_valid_incoming_edge_labels returns empty list with no incoming edges."""
        self.assertEqual(self.schema.get_valid_incoming_edge_labels('Unknown'), [])


class TestGremlinStateMachine(unittest.TestCase):

    def setUp(self):
        """Set up a mock graph schema for testing the state machine."""
        nodes = {
            'A': {'id': 'A', 'type': 'Person'},
            'B': {'id': 'B', 'type': 'Person'},
            'C': {'id': 'C', 'type': 'Company'},
        }
        edges = {
            'A': [
                {'label': 'friend', 'target': 'B'},
                {'label': 'knows', 'target': 'B'},
            ],
            'B': [
                {'label': 'friend', 'target': 'A'},
            ],
            'C': [
                {'label': 'employs', 'target': 'A'},
            ],
        }
        self.schema = InMemoryGraphSchema(nodes, edges)

    def test_vertex_state_options(self):
        """Test that the state machine generates correct, concrete options from a vertex state."""
        state, options = GremlinStateMachine.get_state_and_options(
            "V()", self.schema, "Person"
        )
        self.assertEqual(state, "V")

        # Check for concrete 'out' steps
        self.assertIn("out('friend')", options)
        self.assertIn("out('knows')", options)

        # Check for concrete 'in' steps (Person nodes receive incoming edges)
        self.assertIn("in('friend')", options)
        self.assertIn("in('knows')", options)

        # Check for concrete 'both' steps
        self.assertIn("both('friend')", options)
        self.assertIn("both('knows')", options)

        # Check for non-label steps
        self.assertIn("has('prop','value')", options)
        self.assertIn("stop", options)

    def test_empty_labels(self):
        """Test that no label-based steps are generated if there are no corresponding edges."""
        state, options = GremlinStateMachine.get_state_and_options(
            "V()", self.schema, "Company"
        )
        self.assertEqual(state, "V")
        # Company has outgoing 'employs'/'partner' edges but no incoming edges in this setup.
        self.assertIn("out('employs')", options)
        self.assertNotIn("in('employs')", options)
        self.assertIn("both('employs')", options)

    def test_state_transitions(self):
        """Test that the state machine correctly transitions between states."""
        # V -> E
        state, _ = GremlinStateMachine.get_state_and_options(
            "V().outE('friend')", self.schema, "Person"
        )
        self.assertEqual(state, "E")

        # V -> E -> V
        state, _ = GremlinStateMachine.get_state_and_options(
            "V().outE('friend').inV()", self.schema, "Person"
        )
        self.assertEqual(state, "V")

    def test_invalid_transition(self):
        """Test that an invalid sequence of steps leads to the END state."""
        state, options = GremlinStateMachine.get_state_and_options(
            "V().outV()", self.schema, "Person"
        )
        self.assertEqual(state, "END")
        self.assertEqual(options, [])

    def test_generic_vertex_steps(self):
        """Test that generic (non-label) steps are available at a vertex state."""
        _, options = GremlinStateMachine.get_state_and_options("V()", self.schema, "Person")
        self.assertIn("has('prop','value')", options)
        self.assertIn("dedup()", options)
        self.assertIn("order().by('prop')", options)
        self.assertIn("limit(n)", options)
        self.assertIn("values('prop')", options)

    def test_edge_to_vertex_steps(self):
        """Test that edge-to-vertex steps are available at an edge state."""
        # Transition to an edge state first
        state, options = GremlinStateMachine.get_state_and_options(
            "V().outE('friend')", self.schema, "Person"
        )
        self.assertEqual(state, "E")

        # Now check for edge-specific steps
        self.assertIn("inV()", options)
        self.assertIn("outV()", options)
        self.assertIn("otherV()", options)

    def test_order_by_modifier_keeps_state(self):
        """Test that order().by() modifier does not invalidate state."""
        state, options = GremlinStateMachine.get_state_and_options(
            "V().order().by('prop')", self.schema, "Person"
        )
        self.assertEqual(state, "V")
        self.assertIn("stop", options)
