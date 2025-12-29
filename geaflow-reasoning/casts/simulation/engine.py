"""Simulation engine for managing CASTS strategy cache experiments."""

import random
import re
from typing import Callable, Dict, List, Optional, Tuple

from casts.core.interfaces import DataSource
from casts.core.models import Context
from casts.core.services import StrategyCache
from casts.services.llm_oracle import LLMOracle
from casts.simulation.executor import TraversalExecutor
from casts.simulation.metrics import MetricsCollector


class SimulationEngine:
    """Main engine for running CASTS strategy cache simulations."""

    def __init__(
        self,
        graph: DataSource,
        strategy_cache: StrategyCache,
        llm_oracle: LLMOracle,
        max_depth: int = 10,
        verbose: bool = True,
    ):
        self.graph = graph
        self.strategy_cache = strategy_cache
        self.llm_oracle = llm_oracle
        self.max_depth = max_depth
        self.verbose = verbose
        self.schema = graph.get_schema()
        self.executor = TraversalExecutor(graph, self.schema)

        # Use goal generator provided by the data source instead of hardcoding goals here
        self.goal_generator = graph.get_goal_generator()

    async def run_epoch(
        self, epoch: int, metrics_collector: MetricsCollector
    ) -> List[
        Tuple[str, str, str, int, int | None]
    ]:  # List of (node_id, signature, goal, request_id, parent_step_index)
        """Run a single simulation epoch."""
        print(f"\n--- Epoch {epoch} ---")

        def infer_anchor_node_types(goal_text: str) -> List[str]:
            """Infer likely start node types from a natural-language goal.

            This is intentionally lightweight and schema-driven: it only maps
            tokens in the goal to known schema node types.
            """
            schema_types = list(getattr(self.schema, "node_types", set()) or [])
            if not schema_types:
                return []

            # Case-insensitive matching against known types.
            lower_to_type = {t.lower(): t for t in schema_types}

            # Common patterns in our goal templates.
            single_type_patterns = (
                r"\bStarting\s+from\s+an?\s+([A-Za-z_]+)",
                r"\bStarting\s+with\s+an?\s+([A-Za-z_]+)",
                r"\bGiven\s+an?\s+([A-Za-z_]+)",
                r"\bFor\s+a\s+single\s+([A-Za-z_]+)",
                r"\bFor\s+a\s+given\s+([A-Za-z_]+)",
                r"\bPick\s+a\s+high-risk\s+([A-Za-z_]+)",
            )

            matches: List[str] = []
            for pat in single_type_patterns:
                for m in re.finditer(pat, goal_text, flags=re.IGNORECASE):
                    raw = (m.group(1) or "").strip().strip(".,;:()[]{}\"'")
                    if not raw:
                        continue
                    token = raw.lower()
                    # crude singularization for "accounts" -> "account"
                    if token.endswith("s") and token[:-1] in lower_to_type:
                        token = token[:-1]
                    if token in lower_to_type:
                        matches.append(lower_to_type[token])

            # Two-type pattern used by some goals.
            between = re.search(
                r"\bBetween\s+([A-Za-z_]+)\s+and\s+([A-Za-z_]+)\s+nodes\b",
                goal_text,
                flags=re.IGNORECASE,
            )
            if between:
                for raw in (between.group(1), between.group(2)):
                    token = (raw or "").strip().strip(".,;:()[]{}\"'").lower()
                    if token.endswith("s") and token[:-1] in lower_to_type:
                        token = token[:-1]
                    if token in lower_to_type:
                        matches.append(lower_to_type[token])

            between_one = re.search(
                r"\bBetween\s+([A-Za-z_]+)\s+nodes\b",
                goal_text,
                flags=re.IGNORECASE,
            )
            if between_one:
                raw = between_one.group(1)
                token = (raw or "").strip().strip(".,;:()[]{}\"'").lower()
                if token.endswith("s") and token[:-1] in lower_to_type:
                    token = token[:-1]
                if token in lower_to_type:
                    matches.append(lower_to_type[token])

            # De-dupe while preserving order.
            seen = set()
            result: List[str] = []
            for t in matches:
                if t not in seen:
                    seen.add(t)
                    result.append(t)
            return result

        def weighted_unique_choices(
            population: List[str], weights: List[float], k: int
        ) -> List[str]:
            """Like random.choices, but attempts to avoid duplicates."""
            if k <= 0 or not population:
                return []
            if len(population) == 1:
                return [population[0]] * k

            chosen: List[str] = []
            chosen_set = set()
            attempts = 0
            max_attempts = max(10, k * 10)
            while len(chosen) < k and attempts < max_attempts:
                attempts += 1
                picked = random.choices(population, weights=weights, k=1)[0]
                if picked in chosen_set:
                    continue
                chosen.append(picked)
                chosen_set.add(picked)

            # Fallback: fill remaining with random sample of leftovers.
            if len(chosen) < k:
                leftovers = [n for n in population if n not in chosen_set]
                if leftovers:
                    needed = min(k - len(chosen), len(leftovers))
                    chosen.extend(random.sample(leftovers, k=needed))

            # Final fallback: allow duplicates to reach k.
            if len(chosen) < k:
                needed = k - len(chosen)
                chosen.extend(random.choices(population, weights=weights, k=needed))
            return chosen

        # Generate access pattern following Zipf's law
        node_ids = list(self.graph.nodes.keys())
        zipf_weights = [1.0 / (i + 1) ** 1.2 for i in range(len(node_ids))]
        node_weight_map = {node_id: w for node_id, w in zip(node_ids, zipf_weights, strict=False)}

        # Precompute in-degrees for lightweight structural checks.
        in_degree: Dict[str, int] = dict.fromkeys(node_ids, 0)
        for _src_id, edges in self.graph.edges.items():
            for edge in edges:
                tgt = edge.get("target")
                if tgt in in_degree:
                    in_degree[tgt] += 1

        # Draw a main goal for this epoch from the data source's goal generator.
        # If the inferred anchor types are missing from the current (sub)graph,
        # resample a few times to avoid unavoidable mismatches.
        available_types = {props.get("type") for props in self.graph.nodes.values()}
        epoch_main_goal, epoch_main_rubric = self.goal_generator.select_goal()
        anchor_types = infer_anchor_node_types(epoch_main_goal)
        for _ in range(5):
            if not anchor_types:
                break
            if any(t in available_types for t in anchor_types):
                break
            epoch_main_goal, epoch_main_rubric = self.goal_generator.select_goal()
            anchor_types = infer_anchor_node_types(epoch_main_goal)

        # Filter start candidates to reduce immediate dead-ends (no incident edges).
        # Keep this purely structural (no dataset-specific rules).
        def has_any_incident_edge(node_id: str) -> bool:
            out_deg = len(self.schema.get_valid_edge_labels(node_id))
            return (out_deg + in_degree.get(node_id, 0)) > 0

        if anchor_types:
            start_candidates_by_type = [
                node_id
                for node_id, props in self.graph.nodes.items()
                if props.get("type") in anchor_types
            ]
            start_candidates = [
                node_id for node_id in start_candidates_by_type if has_any_incident_edge(node_id)
            ]
            # If the sampled subgraph has the right type but those nodes have no incident edges,
            # prefer matching the goal's type over falling back to unrelated types.
            if not start_candidates and start_candidates_by_type:
                start_candidates = start_candidates_by_type
        else:
            start_candidates = [node_id for node_id in node_ids if has_any_incident_edge(node_id)]

        # Fallback if graph is very sparse or anchor_types are too restrictive.
        if not start_candidates:
            start_candidates = node_ids

        start_weights = [node_weight_map.get(n, 1.0) for n in start_candidates]

        # Pick start nodes (simultaneous start)
        start_nodes = weighted_unique_choices(start_candidates, start_weights, k=2)

        # Initialize current layer: List of (node_id, signature, goal, request_id, parent_step_index, source_node, edge_label)
        # parent_step_index is for visualization only, tracking which previous step this traverser came from
        # source_node and edge_label track the actual provenance of this traversal step
        current_layer: List[Tuple[str, str, str, int, int | None, str | None, str | None]] = []
        for node_id in start_nodes:
            node_type = self.graph.nodes[node_id].get("type")
            # With high probability, reuse the epoch main goal; otherwise, sample another goal
            if random.random() < 0.8:
                goal_text = epoch_main_goal
                rubric = epoch_main_rubric
            else:
                goal_text, rubric = self.goal_generator.select_goal(node_type=node_type)
                # Avoid obvious anchor mismatches (e.g., goal anchored on Company but starting from Account)
                # when the goal text happens to mention the node_type somewhere else.
                for _ in range(5):
                    inferred = infer_anchor_node_types(goal_text)
                    if (not inferred) or (node_type in inferred):
                        break
                    goal_text, rubric = self.goal_generator.select_goal(node_type=node_type)

            # Initialize path tracking
            request_id = metrics_collector.initialize_path(epoch, node_id, self.graph.nodes[node_id], goal_text, rubric)
            # Root nodes have no parent step, source_node, or edge_label (all None)
            current_layer.append((node_id, "V()", goal_text, request_id, None, None, None))

        return current_layer

    async def execute_tick(
        self,
        tick: int,
        current_layer: List[Tuple[str, str, str, int, int | None, str | None, str | None]],
        metrics_collector: MetricsCollector,
        edge_history: Dict[Tuple[str, str], int],
    ) -> Tuple[
        List[Tuple[str, str, str, int, int | None, str | None, str | None]],
        Dict[Tuple[str, str], int],
    ]:
        """Execute a single simulation tick for all active traversers."""
        if self.verbose:
            print(f"\n[Tick {tick}] Processing {len(current_layer)} active traversers")

        next_layer = []

        for idx, traversal_state in enumerate(current_layer):
            (
                current_node_id,
                current_signature,
                current_goal,
                request_id,
                parent_step_index,
                source_node,
                edge_label,
            ) = traversal_state
            node = self.graph.nodes[current_node_id]

            # Use stored provenance information instead of searching the graph
            # This ensures we log the actual edge that was traversed, not a random one
            if self.verbose:
                print(
                    f"  [{idx + 1}/{len(current_layer)}] Node {current_node_id}({node['type']}) | "
                    f"s='{current_signature}' | g='{current_goal}'"
                )
            if source_node is not None and edge_label is not None and self.verbose:
                print(f"    ↑ via {edge_label} from {source_node}")

            # Create context and find strategy
            context = Context(
                structural_signature=current_signature, properties=node, goal=current_goal
            )

            decision, sku, match_type = await self.strategy_cache.find_strategy(context)
            # Use match_type (Tier1/Tier2) to determine cache hit vs miss,
            # rather than truthiness of the decision string.
            is_cache_hit = match_type in ("Tier1", "Tier2")
            final_decision = decision

            # Record step in path
            # parent_step_index is for visualization only, passed from current_layer
            # Use stored provenance information (source_node, edge_label) instead of searching
            metrics_collector.record_path_step(
                request_id=request_id,
                tick=tick,
                node_id=current_node_id,
                parent_node=source_node,
                parent_step_index=parent_step_index,
                edge_label=edge_label,
                structural_signature=current_signature,
                goal=current_goal,
                properties=node,
                match_type=match_type,
                sku_id=getattr(sku, "id", None) if sku else None,
                decision=None,  # Will be updated after execution
            )

            # Record metrics (hit type or miss)
            metrics_collector.record_step(match_type)

            if is_cache_hit:
                if self.verbose:
                    if match_type == "Tier1":
                        if sku is not None:
                            print(
                                f"    → [Hit T1] SKU {sku.id} | {decision} "
                                f"(confidence={sku.confidence_score:.1f}, "
                                f"complexity={sku.logic_complexity})"
                            )
                    elif match_type == "Tier2":
                        if sku is not None:
                            print(
                                f"    → [Hit T2] SKU {sku.id} | {decision} "
                                f"(confidence={sku.confidence_score:.1f}, "
                                f"complexity={sku.logic_complexity})"
                            )

                # Simulate execution success/failure
                execution_success = random.random() > 0.05
                if not execution_success:
                    metrics_collector.record_execution_failure()
                    if self.verbose:
                        print("      [!] Execution failed, confidence penalty applied")

                if sku is not None:
                    self.strategy_cache.update_confidence(sku, execution_success)
            else:
                # Cache miss - generate new SKU via LLM
                new_sku = await self.llm_oracle.generate_sku(context, self.schema)
                final_decision = new_sku.decision_template

                # Check for duplicate and merge or add
                exists = False
                for existing in self.strategy_cache.knowledge_base:
                    if (
                        existing.structural_signature == new_sku.structural_signature
                        and existing.goal_template == new_sku.goal_template
                    ):
                        existing.confidence_score += 1
                        exists = True
                        if self.verbose:
                            print(
                                f"    → [LLM] Merge into SKU {existing.id} "
                                f"(confidence={existing.confidence_score:.1f})"
                            )
                        sku = existing
                        match_type = "Tier1"
                        break

                if not exists:
                    self.strategy_cache.add_sku(new_sku)
                    sku = new_sku
                    match_type = "Tier1"
                    if self.verbose:
                        print(
                            f"    → [LLM] New SKU {new_sku.id} | {final_decision} "
                            f"(confidence={new_sku.confidence_score:.1f}, "
                            f"complexity={new_sku.logic_complexity})"
                        )

            # Update the recorded step with final decision
            if metrics_collector.paths[request_id]["steps"]:
                metrics_collector.paths[request_id]["steps"][-1]["decision"] = final_decision

            # Execute the decision
            if final_decision:
                next_nodes = await self.executor.execute_decision(
                    current_node_id, final_decision, current_signature
                )

                if self.verbose:
                    print(f"    → Execute: {final_decision} → {len(next_nodes)} targets")
                    if not next_nodes:
                        print(f"    → No valid targets for {final_decision}, path terminates")

                for next_node_id, next_signature, traversed_edge in next_nodes:
                    # For visualization: the parent step index for next layer
                    # is the index of this step
                    # Find the index of the step we just recorded
                    steps = metrics_collector.paths[request_id]["steps"]
                    this_step_index = len(steps) - 1

                    # Extract source node and edge label from traversed edge info
                    # traversed_edge is a tuple of (source_node_id, edge_label)
                    next_source_node, next_edge_label = (
                        traversed_edge if traversed_edge else (None, None)
                    )

                    next_layer.append(
                        (
                            next_node_id,
                            next_signature,
                            current_goal,
                            request_id,
                            this_step_index,
                            next_source_node,
                            next_edge_label,
                        )
                    )

                    # Record edge traversal for visualization
                    if (current_node_id, next_node_id) not in edge_history:
                        edge_history[(current_node_id, next_node_id)] = tick

        return next_layer, edge_history

    async def run_simulation(
        self,
        num_epochs: int = 2,
        metrics_collector: Optional[MetricsCollector] = None,
        on_request_completed: Optional[Callable[[int, MetricsCollector], None]] = None,
    ) -> MetricsCollector:
        """Run complete simulation across multiple epochs."""
        if metrics_collector is None:
            metrics_collector = MetricsCollector()

        print("=== CASTS Strategy Cache Simulation ===")
        source_label = getattr(self.graph, "source_label", "synthetic")
        distribution_note = "Zipf distribution" if source_label == "synthetic" else "real dataset"
        print(f"1. Graph Data: {len(self.graph.nodes)} nodes ({distribution_note})")

        type_counts = {}
        for node in self.graph.nodes.values():
            node_type = node["type"]
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
        print(f"   Node distribution: {type_counts}")

        print("2. Embedding Service: OpenRouter API")
        print("3. Strategy Cache: Initialized")
        print(f"4. Starting simulation ({num_epochs} epochs)...")

        for epoch in range(1, num_epochs + 1):
            current_layer = await self.run_epoch(epoch, metrics_collector)

            tick = 0
            visited_history = set()
            edge_history = {}

            active_request_ids = {layer[3] for layer in current_layer}

            while current_layer:
                tick += 1

                # Store the active requests before the tick
                requests_before_tick = {layer[3] for layer in current_layer}

                current_layer, edge_history = await self.execute_tick(
                    tick, current_layer, metrics_collector, edge_history
                )

                # Determine completed requests
                requests_after_tick = {layer[3] for layer in current_layer}
                completed_requests = requests_before_tick - requests_after_tick

                if completed_requests and on_request_completed:
                    for request_id in completed_requests:
                        on_request_completed(request_id, metrics_collector)

                # Update visited history
                for node_id, _, _, _, _, _, _ in current_layer:
                    visited_history.add(node_id)

                if tick > self.max_depth:
                    print(
                        f"    [Depth limit reached (max_depth={self.max_depth}), "
                        f"ending epoch {epoch}]"
                    )
                    break

            # Cleanup low confidence SKUs at end of epoch
            evicted = len(
                [sku for sku in self.strategy_cache.knowledge_base if sku.confidence_score < 0.5]
            )
            self.strategy_cache.cleanup_low_confidence_skus()
            metrics_collector.record_sku_eviction(evicted)

            if evicted > 0:
                print(f"  [Cleanup] Evicted {evicted} low-confidence SKUs")

        return metrics_collector
