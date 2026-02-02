"""Traversal executor for simulating graph traversal decisions."""

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from casts.core.interfaces import DataSource, GraphSchema


class TraversalExecutor:
    """Executes traversal decisions on the graph and manages traversal state."""

    def __init__(self, graph: DataSource, schema: GraphSchema):
        self.graph = graph
        self.schema = schema
        # Track visited nodes for each request to support simplePath()
        self._path_history: Dict[int, Set[str]] = {}

    def _ensure_path_history(self, request_id: int, current_node_id: str) -> Set[str]:
        """Ensure path history is initialized for a request and seed current node."""
        if request_id not in self._path_history:
            self._path_history[request_id] = {current_node_id}
        return self._path_history[request_id]

    async def execute_decision(
        self, current_node_id: str, decision: str, current_signature: str,
        request_id: Optional[int] = None
    ) -> List[Tuple[str, str, Optional[Tuple[Any, ...]]]]:
        """
        Execute a traversal decision and return next nodes with updated signatures.

        Args:
            current_node_id: Current node ID
            decision: Traversal decision string (e.g., "out('friend')")
            current_signature: Current traversal signature
            request_id: Request ID for tracking simplePath history

        Returns:
            List of (next_node_id, next_signature, traversed_edge) tuples
            where traversed_edge is (source_node_id, edge_label) or None
        """
        next_nodes: List[Tuple[str, Optional[str], Optional[Tuple[str, str]]]] = []

        # Check if simplePath is enabled for this traversal
        has_simple_path = "simplePath()" in current_signature

        if request_id is not None:
            self._ensure_path_history(request_id, current_node_id)

        try:
            # 1) Vertex out/in traversal (follow edges to adjacent nodes)
            if decision.startswith("out('"):
                label = decision.split("'")[1]
                neighbors = self.graph.edges.get(current_node_id, [])
                for edge in neighbors:
                    if edge["label"] == label:
                        next_nodes.append((edge["target"], None, (current_node_id, label)))

            elif decision.startswith("in('"):
                label = decision.split("'")[1]
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            next_nodes.append((src_id, None, (src_id, label)))

            # 2) Bidirectional traversal both('label')
            elif decision.startswith("both('"):
                label = decision.split("'")[1]
                for edge in self.graph.edges.get(current_node_id, []):
                    if edge["label"] == label:
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            next_nodes.append((src_id, None, (src_id, label)))

            # 3) Edge traversal outE/inE: simplified to out/in for simulation
            elif decision.startswith("outE('"):
                label = decision.split("'")[1]
                neighbors = self.graph.edges.get(current_node_id, [])
                for edge in neighbors:
                    if edge["label"] == label:
                        next_nodes.append((edge["target"], None, (current_node_id, label)))

            elif decision.startswith("inE('"):
                label = decision.split("'")[1]
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            next_nodes.append((src_id, None, (src_id, label)))

            elif decision.startswith("bothE('"):
                label = decision.split("'")[1]
                for edge in self.graph.edges.get(current_node_id, []):
                    if edge["label"] == label:
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            next_nodes.append((src_id, None, (src_id, label)))

            # 3) Vertex property filtering has('prop','value')
            elif decision.startswith("has("):
                m = re.match(r"^has\('([^']+)'\s*,\s*'([^']*)'\)$", decision)
                if m:
                    prop, value = m.group(1), m.group(2)
                    node = self.graph.nodes[current_node_id]
                    node_val = str(node.get(prop, ""))
                    matched = node_val == value
                    if matched:
                        next_nodes.append((current_node_id, None, None))

            # 4) simplePath(): Filter step that enables path uniqueness
            elif decision == "simplePath()":
                # simplePath is a filter that passes through the current node
                # but marks the path for deduplication in the final step
                next_nodes.append((current_node_id, None, None))

            # 5) dedup(): At single-node granularity, this is a no-op
            elif decision.startswith("dedup"):
                next_nodes.append((current_node_id, None, None))

            # 6) Edge-to-vertex navigation: inV(), outV(), otherV()
            elif decision in ("inV()", "outV()", "otherV()"):
                next_nodes.append((current_node_id, None, None))

            # 7) Property value extraction: values('prop') or values()
            elif decision.startswith("values("):
                next_nodes.append((current_node_id, None, None))

            # 8) Result ordering: order() or order().by('prop')
            elif decision.startswith("order("):
                next_nodes.append((current_node_id, None, None))

            # 9) Result limiting: limit(n)
            elif decision.startswith("limit("):
                next_nodes.append((current_node_id, None, None))

            # 5) stop: Terminate traversal
            elif decision == "stop":
                pass

        except (KeyError, ValueError, TypeError, RuntimeError, AttributeError):
            pass

        # Build final signatures for all nodes
        final_nodes: List[Tuple[str, str, Optional[Tuple[Any, ...]]]] = []
        for next_node_id, _, traversed_edge in next_nodes:
            # Always append the full decision to create a canonical, Level-2 signature.
            # The abstraction logic is now handled by the StrategyCache during matching.
            next_signature = f"{current_signature}.{decision}"

            # If simplePath is enabled, filter out already-visited nodes
            if has_simple_path and request_id is not None:
                history = self._ensure_path_history(request_id, current_node_id)
                # Only enforce simplePath on traversal steps that move along an edge.
                if traversed_edge is not None and next_node_id in history:
                    continue
                history.add(next_node_id)

            if request_id is not None and not has_simple_path:
                self._ensure_path_history(request_id, current_node_id).add(next_node_id)

            final_nodes.append((next_node_id, next_signature, traversed_edge))

        return final_nodes

    def clear_path_history(self, request_id: int):
        """Clear the path history for a completed request.

        This should be called when a traversal request completes to free memory.

        Args:
            request_id: The ID of the completed request
        """
        if request_id in self._path_history:
            del self._path_history[request_id]
