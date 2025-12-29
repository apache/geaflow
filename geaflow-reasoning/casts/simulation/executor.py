"""Traversal executor for simulating graph traversal decisions."""

import re
from typing import List, Tuple

from casts.core.interfaces import DataSource, GraphSchema


class TraversalExecutor:
    """Executes traversal decisions on the graph and manages traversal state."""

    def __init__(self, graph: DataSource, schema: GraphSchema):
        self.graph = graph
        self.schema = schema

    async def execute_decision(
        self, current_node_id: str, decision: str, current_signature: str
    ) -> List[Tuple[str, str, tuple | None]]:
        """
        Execute a traversal decision and return next nodes with updated signatures.

        Args:
            current_node_id: Current node ID
            decision: Traversal decision string (e.g., "out('friend')")
            current_signature: Current traversal signature

        Returns:
            List of (next_node_id, next_signature, traversed_edge) tuples
            where traversed_edge is (source_node_id, edge_label) or None
        """
        next_nodes = []
        is_filter_step = False
        direction = None

        try:
            # 1) Vertex out/in traversal (follow edges to adjacent nodes)
            if decision.startswith("out('"):
                direction = "out"
                label = decision.split("'")[1]
                neighbors = self.graph.edges.get(current_node_id, [])
                for edge in neighbors:
                    if edge["label"] == label:
                        # Store the actual edge that was traversed
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                print(f"    → Execute: out('{label}') → {len(next_nodes)} targets")

            elif decision.startswith("in('"):
                direction = "in"
                label = decision.split("'")[1]
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            # Store the actual edge that was traversed
                            next_nodes.append((src_id, None, (src_id, label)))
                print(f"    → Execute: in('{label}') → {len(next_nodes)} sources")

            # 2) Bidirectional traversal both('label')
            elif decision.startswith("both('"):
                direction = "both"
                label = decision.split("'")[1]
                # Outgoing edges with label
                for edge in self.graph.edges.get(current_node_id, []):
                    if edge["label"] == label:
                        # Store the actual edge that was traversed
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                # Incoming edges with label
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            # Store the actual edge that was traversed
                            next_nodes.append((src_id, None, (src_id, label)))
                print(f"    → Execute: both('{label}') → {len(next_nodes)} nodes")

            # 3) Edge traversal outE/inE: simplified to out/in for simulation
            elif decision.startswith("outE('"):
                direction = "out"
                label = decision.split("'")[1]
                neighbors = self.graph.edges.get(current_node_id, [])
                for edge in neighbors:
                    if edge["label"] == label:
                        # Store the actual edge that was traversed
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                print(
                    f"    → Execute: outE('{label}') ~ out('{label}') → {len(next_nodes)} targets"
                )

            elif decision.startswith("inE('"):
                direction = "in"
                label = decision.split("'")[1]
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            # Store the actual edge that was traversed
                            next_nodes.append((src_id, None, (src_id, label)))
                print(f"    → Execute: inE('{label}') ~ in('{label}') → {len(next_nodes)} sources")

            elif decision.startswith("bothE('"):
                direction = "both"
                label = decision.split("'")[1]
                # Outgoing edges with label
                for edge in self.graph.edges.get(current_node_id, []):
                    if edge["label"] == label:
                        # Store the actual edge that was traversed
                        next_nodes.append((edge["target"], None, (current_node_id, label)))
                # Incoming edges with label
                for src_id, edges in self.graph.edges.items():
                    for edge in edges:
                        if edge["target"] == current_node_id and edge["label"] == label:
                            # Store the actual edge that was traversed
                            next_nodes.append((src_id, None, (src_id, label)))
                print(
                    f"    → Execute: bothE('{label}') ~ both('{label}') → {len(next_nodes)} nodes"
                )

            # 3) Vertex property filtering has('prop','value')
            elif decision.startswith("has("):
                is_filter_step = True
                m = re.match(r"^has\('([^']+)'\s*,\s*'([^']*)'\)$", decision)
                if m:
                    prop, value = m.group(1), m.group(2)
                    node = self.graph.nodes[current_node_id]
                    node_val = str(node.get(prop, ""))
                    matched = node_val == value
                    print(
                        "    → Execute: has("
                        f"'{prop}','{value}') on node {current_node_id} => {matched}"
                    )
                    if matched:
                        # Continue with current node, no edge traversed
                        next_nodes.append((current_node_id, None, None))
                    # else: filter out (no nodes added)
                else:
                    print(f"    → Execute: parse error for has-step '{decision}'")

            # 4) dedup(): At single-node granularity, this is a no-op
            elif decision.startswith("dedup"):
                is_filter_step = True
                print("    → Execute: dedup() (no-op at single-node granularity)")
                # Continue with current node, no edge traversed
                next_nodes.append((current_node_id, None, None))

            # 5) stop: Terminate traversal
            elif decision == "stop":
                print("    → Execute: stop (terminates this path)")
                # No nodes to add

            else:
                print(f"    → Execute: unsupported decision '{decision}'")

        except (KeyError, ValueError, TypeError, RuntimeError, AttributeError) as exc:
            print(f"    → Execute: error executing '{decision}': {exc}")

        # Build final signatures for all nodes
        final_nodes = []
        for next_node_id, _, traversed_edge in next_nodes:
            if is_filter_step:
                # Filter steps: Keep structure, just add filter marker
                next_signature = f"{current_signature}.filter()"
            else:
                # Structural traversal: Extend signature with direction
                if direction is not None:
                    next_signature = f"{current_signature}.{direction}()"
                else:
                    next_signature = current_signature
            final_nodes.append((next_node_id, next_signature, traversed_edge))

        if not final_nodes and decision not in [None, "stop"]:
            print(f"    → No valid targets for {decision}, path terminates")

        return final_nodes
