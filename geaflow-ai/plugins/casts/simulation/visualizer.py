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

"""Visualization and reporting for CASTS simulation results."""

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import networkx as nx

from core.interfaces import DataSource
from core.models import Context, StrategyKnowledgeUnit
from core.strategy_cache import StrategyCache
from simulation.metrics import PathInfo, PathStep, SimulationMetrics
from utils.helpers import (
    calculate_dynamic_similarity_threshold,
    calculate_tier2_threshold,
)


class SimulationVisualizer:
    """Handles visualization and reporting of simulation results."""

    @staticmethod
    def generate_mermaid_diagram(request_id: int, path_info: PathInfo) -> str:
        """Generate a Mermaid flowchart for a single request's traversal path."""
        steps: list[PathStep] = path_info["steps"]

        lines = [
            "graph TD",
            f"    %% Request {request_id}: Goal = {path_info['goal']}",
            f"    %% Start Node: {path_info['start_node']}, Epoch: {path_info['epoch']}",
        ]

        # Build a stable mapping from (tick, node_id) to step index
        node_index: dict[tuple, int] = {}
        for idx, step in enumerate(steps):
            node_index[(step["tick"], step["node"])] = idx

        # Create nodes
        for idx, step in enumerate(steps):
            step_var = f"Step{idx}"
            node_label = f"{step['node']}:{step['p']['type']}"
            decision = step["decision"] or "None"
            match_type = step["match_type"] or "None"
            tick = step["tick"]

            lines.append(
                f'    {step_var}["Tick {tick}: {node_label}<br/>'
                f"Decision: {decision}<br/>"
                f"Match: {match_type}<br/>"
                f'SKU: {step["sku_id"]}"]'
            )

        # Create edges using explicit parent_step_index when available
        for idx, step in enumerate(steps):
            parent_idx = step.get("parent_step_index")
            edge_label = step.get("edge_label")
            # For visualization only: if a parent_step_index was recorded,
            # draw an edge from that step to the current step.
            if parent_idx is not None:
                if edge_label:
                    lines.append(f"    Step{parent_idx} -->|{edge_label}| Step{idx}")
                else:
                    lines.append(f"    Step{parent_idx} --> Step{idx}")

        return "\n".join(lines)

    @staticmethod
    def print_traversal_paths(paths: dict[int, PathInfo]):
        """Print both textual paths and Mermaid diagrams for all requests."""
        print("\n=== Traversal Paths for Each Request ===")
        for request_id, path_info in paths.items():
            print(
                f"\n[Req {request_id}] Epoch={path_info['epoch']} "
                f"StartNode={path_info['start_node']} Goal='{path_info['goal']}'"
            )

            # Print textual path
            for step in path_info["steps"]:
                properties_brief = {"id": step["p"]["id"], "type": step["p"]["type"]}
                print(
                    f"  - Tick {step['tick']}: "
                    f"s='{step['s']}' "
                    f"p={properties_brief} "
                    f"g='{step['g']}' "
                    f"node={step['node']} "
                    f"match={step['match_type']} "
                    f"sku={step['sku_id']} "
                    f"decision={step['decision']}"
                )

            # Print Mermaid diagram
            print("\n  Mermaid diagram:")
            print("  ```mermaid")
            print(SimulationVisualizer.generate_mermaid_diagram(request_id, path_info))
            print("  ```")
            print("-" * 40)

    @staticmethod
    def print_knowledge_base_state(sorted_skus: list[StrategyKnowledgeUnit]):
        """Print final knowledge base state (Top 5 SKUs by confidence)."""
        print("\n=== Final Knowledge Base State (Top 5 SKUs) ===")
        for sku in sorted_skus[:5]:
            print(f"SKU {sku.id}:")
            print(f"  - structural_signature: {sku.structural_signature}")
            vector_head = sku.property_vector[:3]
            rounded_head = [round(x, 3) for x in vector_head]
            vector_summary = (
                f"Vector(dim={len(sku.property_vector)}, head={rounded_head}...)"
            )
            print(f"  - property_vector: {vector_summary}")
            print(f"  - goal_template: {sku.goal_template}")
            print(f"  - decision_template: {sku.decision_template}")
            print(f"  - confidence_score: {sku.confidence_score}")
            print(f"  - logic_complexity: {sku.logic_complexity}")
            print("-" * 50)

    @staticmethod
    async def print_tier2_diagnostics(
        cache: StrategyCache, sorted_skus: list[StrategyKnowledgeUnit]
    ):
        """Print Tier2 threshold diagnostics and self-test."""
        print("\n=== Tier2 Threshold Diagnostics (Dynamic Similarity) ===")
        if sorted_skus:
            sample_sku = sorted_skus[0]
            delta_threshold = calculate_dynamic_similarity_threshold(
                sample_sku, cache.similarity_kappa, cache.similarity_beta
            )
            tier2_threshold = calculate_tier2_threshold(
                cache.min_confidence_threshold, cache.tier2_gamma
            )
            print(f"Sample SKU: {sample_sku.id}")
            print(f"  confidence = {sample_sku.confidence_score:.1f}")
            print(f"  logic_complexity = {sample_sku.logic_complexity}")
            print(
                "  tier2_threshold(min_confidence="
                f"{cache.min_confidence_threshold}) = {tier2_threshold:.1f}"
            )
            print(
                f"  dynamic_threshold = {delta_threshold:.4f} "
                f"(similarity must be >= this to trigger Tier2)"
            )

        if sorted_skus:
            print("\n=== Tier2 Logic Self-Test (Synthetic Neighbor Vector) ===")
            sku = sorted_skus[0]

            # Temporarily override embedding service to return known vector
            original_embed = cache.embed_service.embed_properties

            async def fake_embed(props):
                return sku.property_vector

            cache.embed_service.embed_properties = fake_embed  # type: ignore[method-assign]

            # Create test context with same properties as SKU
            test_context = Context(
                structural_signature=sku.structural_signature,
                properties={"type": "NonExistingType"},  # Different type but same vector
                goal=sku.goal_template,
            )

            decision, used_sku, match_type = await cache.find_strategy(
                test_context, skip_tier1=True
            )

            # Restore original embedding service
            cache.embed_service.embed_properties = original_embed  # type: ignore[method-assign]

            print(
                "  Synthetic test context: structural_signature="
                f"'{test_context.structural_signature}', goal='{test_context.goal}'"
            )
            print(
                f"  Result: decision={decision}, match_type={match_type}, "
                f"used_sku={getattr(used_sku, 'id', None) if used_sku else None}"
            )
            print("  (If match_type == 'Tier2', Tier2 logic is working correctly)")

    @staticmethod
    async def print_all_results(
        paths: dict[int, PathInfo],
        metrics: SimulationMetrics,
        cache: StrategyCache,
        sorted_skus: list[StrategyKnowledgeUnit],
        graph: DataSource | None = None,
        show_plots: bool = True,
    ):
        """Master function to print all simulation results.

        Args:
            paths: Dictionary of path information for all requests
            metrics: Simulation metrics object
            cache: Strategy cache instance
            sorted_skus: Sorted list of SKUs
            graph: The graph object for visualization (optional)
            show_plots: Whether to display matplotlib plots
        """
        print("\n=== Simulation Summary ===")
        print(f"Total Steps: {metrics.total_steps}")
        print(f"LLM Calls: {metrics.llm_calls}")
        print(f"Tier 1 Hits: {metrics.tier1_hits}")
        print(f"Tier 2 Hits: {metrics.tier2_hits}")
        print(f"Execution Failures: {metrics.execution_failures}")
        print(f"SKU Evictions: {metrics.sku_evictions}")
        print(f"Overall Hit Rate: {metrics.hit_rate:.2%}")

        SimulationVisualizer.print_knowledge_base_state(sorted_skus)
        await SimulationVisualizer.print_tier2_diagnostics(cache, sorted_skus)
        SimulationVisualizer.print_traversal_paths(paths)

        # Generate matplotlib visualizations if graph is provided
        if graph is not None:
            SimulationVisualizer.plot_all_traversal_paths(
                paths=paths, graph=graph, show=show_plots
            )

    @staticmethod
    def plot_traversal_path(
        request_id: int, path_info: PathInfo, graph: DataSource, show: bool = True
    ):
        """Generate a matplotlib visualization for a single request's traversal path.

        Args:
            request_id: The request ID
            path_info: Path information containing steps
            graph: The graph object containing nodes and edges
            show: Whether to display the plot immediately

        Returns:
            The matplotlib Figure when ``show`` is True, otherwise ``None``.
        """
        steps: list[PathStep] = path_info["steps"]

        # Create a directed graph for visualization
        G: nx.DiGraph = nx.DiGraph()

        # Track visited nodes and edges
        visited_nodes = set()
        traversal_edges = []

        # Add all nodes from the original graph
        for node_id, node_data in graph.nodes.items():
            G.add_node(node_id, **node_data)

        # Add all edges from the original graph
        for src_id, edge_list in graph.edges.items():
            for edge in edge_list:
                G.add_edge(src_id, edge["target"], label=edge["label"])

        # Mark traversal path nodes and edges
        traversal_edge_labels = {}
        for step in steps:
            node_id = step["node"]
            visited_nodes.add(node_id)

            # Add traversal edges based on parent_step_index
            parent_idx = step.get("parent_step_index")
            edge_label = step.get("edge_label")
            if parent_idx is not None and parent_idx < len(steps):
                parent_node = steps[parent_idx]["node"]
                traversal_edges.append((parent_node, node_id))
                # Store the edge label for this traversed edge
                if edge_label:
                    traversal_edge_labels[(parent_node, node_id)] = edge_label

        # Create layout
        pos = nx.spring_layout(G, k=1.5, iterations=50)

        # Create figure
        fig, ax = plt.subplots(figsize=(12, 8))

        # Draw all nodes in light gray
        all_nodes = list(G.nodes())
        node_colors = []
        for node in all_nodes:
            if node == path_info["start_node"]:
                node_colors.append("#FF6B6B")  # Color A: Red for start node
            elif node in visited_nodes:
                node_colors.append("#4ECDC4")  # Color B: Teal for visited nodes
            else:
                node_colors.append("#E0E0E0")  # Light gray for unvisited nodes

        # Draw nodes
        nx.draw_networkx_nodes(
            G, pos, nodelist=all_nodes, node_color=node_colors, node_size=500, alpha=0.8, ax=ax
        )

        # Draw all edges in light gray
        nx.draw_networkx_edges(
            G,
            pos,
            edge_color="#CCCCCC",
            width=1,
            alpha=0.3,
            arrows=True,
            arrowsize=20,
            ax=ax,
        )

        # Draw traversal edges in color B (teal)
        if traversal_edges:
            nx.draw_networkx_edges(
                G,
                pos,
                edgelist=traversal_edges,
                edge_color="#4ECDC4",
                width=2.5,
                alpha=0.8,
                arrows=True,
                arrowsize=25,
                ax=ax,
            )

        # Add labels
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight="bold", ax=ax)

        # Add edge labels for all edges
        edge_labels = nx.get_edge_attributes(G, "label")
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=6, ax=ax)

        # Highlight traversal edge labels
        if traversal_edge_labels:
            # Draw traversal edge labels in bold and color B (teal)
            nx.draw_networkx_edge_labels(
                G,
                pos,
                edge_labels=traversal_edge_labels,
                font_size=7,
                font_color="#4ECDC4",
                font_weight="bold",
                ax=ax,
            )

        # Set title
        ax.set_title(
            f"CASTS Traversal Path - Request {request_id}\n"
            f"Goal: {path_info['goal']} | Epoch: {path_info['epoch']}",
            fontsize=12,
            fontweight="bold",
            pad=20,
        )

        # Add legend
        legend_elements = [
            Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor="#FF6B6B",
                markersize=10,
                label="Start Node",
            ),
            Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor="#4ECDC4",
                markersize=10,
                label="Visited Nodes",
            ),
            Line2D([0], [0], color="#4ECDC4", linewidth=2.5, label="Traversal Path"),
        ]
        ax.legend(handles=legend_elements, loc="upper right")

        # Remove axes
        ax.set_axis_off()

        if not show:
            filename = f"casts_traversal_path_req_{request_id}.png"
            plt.savefig(filename, dpi=150, bbox_inches="tight")
            print(f"  Saved visualization to {filename}")
            plt.close(fig)
            return None

        return fig

    @staticmethod
    def plot_all_traversal_paths(
        paths: dict[int, PathInfo], graph: DataSource, show: bool = True
    ):
        """Generate matplotlib visualizations for all requests' traversal paths.

        Args:
            paths: Dictionary of path information for all requests
            graph: The graph object containing nodes and edges
            show: Whether to display plots (False for batch processing)
        """
        print("\n=== Matplotlib Visualizations for Each Request ===")
        figures = []

        for request_id, path_info in paths.items():
            print(f"\nGenerating visualization for Request {request_id}...")
            fig = SimulationVisualizer.plot_traversal_path(
                request_id=request_id, path_info=path_info, graph=graph, show=show
            )
            if show and fig is not None:
                figures.append(fig)
                plt.show(block=False)

        if show and figures:
            print("\nDisplaying traversal plots (close plot windows to continue)...")
            plt.show(block=True)
            for fig in figures:
                plt.close(fig)
        elif not show:
            print("\nAll visualizations saved as PNG files.")
