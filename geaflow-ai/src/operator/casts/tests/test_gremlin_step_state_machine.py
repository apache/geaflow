"""
本模块包含对 CASTS 推理引擎核心逻辑的单元测试，主要关注
`InMemoryGraphSchema` 和 `GremlinStateMachine` 的正确性。

所有测试都设计为完全独立于任何外部 LLM 调用，以确保图遍历和
状态管理的基础逻辑是正确、确定且健壮的。

---

### 测试策略与案例设计思考

1. **`TestGraphSchema` (图 Schema 测试)**:
   - **目标**: 验证 Schema 提取逻辑能否正确识别并分离每个节点的
     “出边”和“入边”标签。
   - **方法**: 在 `setUp` 中构建一个包含多种连接关系的模拟图。测试断言
     `get_valid_outgoing_edge_labels` (出边) 和
     `get_valid_incoming_edge_labels` (入边) 为不同节点返回预期标签。
   - **核心测试案例**:
     - **节点 `A`**: 同时有出边 (`friend`, `works_for`) 和入边
       (`friend`, `employs`)，用于测试混合情况。
     - **节点 `B`**: 主要测试其出边 (`friend` 到 `A`)。
     - **节点 `D`**: 只有入边 (`partner` 来自 `C`)，没有出边。
       用于验证 `get_valid_outgoing_edge_labels` 返回空列表，
       确认修复“错误回退到全局标签”的严重 bug。
     - **入边/出边分离**: 确保 `get_valid_outgoing_edge_labels` 和
       `get_valid_incoming_edge_labels` 返回的标签列表严格区分且正确。

2. **`TestGremlinStateMachine` (Gremlin 状态机测试)**:
   - **目标**: 验证状态机能否正确与 `GraphSchema` 集成，并根据
     当前节点上下文生成合法的 Gremlin 步骤列表，同时验证状态转换。
   - **方法**: 构建模拟 Schema，使用不同遍历路径
     (`structural_signature`) 和节点 ID 调用 `get_state_and_options`。
   - **核心测试案例**:
     - **Schema 集成 (`test_vertex_state_options`)**:
       - **思考**: 不再检查泛型 `out('label')`，而是检查 Schema
         派生出的具体步骤。
       - **验证**: 对于节点 `A`（`friend` 与 `knows` 出边），
         选项中必须包含 `out('friend')` 和 `out('knows')`。
     - **方向性 (`test_vertex_state_options`)**:
       - **思考**: 确认 `in` 和 `out` 步骤基于正确边方向生成。
       - **验证**: 对于节点 `A`，有来自 `B` 的 `friend` 入边，
         `in('friend')` 必须合法；没有 `knows` 入边，
         `in('knows')` 不能出现。
     - **空标签 (`test_empty_labels`)**:
       - **思考**: 某方向无特定标签时不生成对应步骤。
       - **验证**: 节点 `B` 无 `knows` 出边，因此 `out('knows')`
         不应出现，`in('knows')` 与 `both('knows')` 仍可合法。
     - **状态转换 (`test_state_transitions`)**:
       - **思考**: 验证状态机遵循 Gremlin 流转（V -> E -> V）。
       - **验证**: `V().outE(...)` 后为 `E`；
         `V().outE(...).inV()` 后回到 `V`。
     - **无效转换 (`test_invalid_transition`)**:
       - **思考**: 确保语法严格性。
       - **验证**: `V().outV()` 必须导致 `END` 并返回空选项列表。
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
