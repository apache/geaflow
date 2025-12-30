"""Metrics collection and analysis for CASTS simulations."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class SimulationMetrics:
    """Comprehensive metrics for CASTS simulation performance analysis."""

    total_steps: int = 0
    llm_calls: int = 0
    tier1_hits: int = 0
    tier2_hits: int = 0
    misses: int = 0
    execution_failures: int = 0
    sku_evictions: int = 0

    @property
    def total_hits(self) -> int:
        """Total cache hits (Tier1 + Tier2)."""
        return self.tier1_hits + self.tier2_hits

    @property
    def hit_rate(self) -> float:
        """Overall cache hit rate."""
        if self.total_steps == 0:
            return 0.0
        return self.total_hits / self.total_steps

    @property
    def tier1_hit_rate(self) -> float:
        """Tier 1 hit rate."""
        if self.total_steps == 0:
            return 0.0
        return self.tier1_hits / self.total_steps

    @property
    def tier2_hit_rate(self) -> float:
        """Tier 2 hit rate."""
        if self.total_steps == 0:
            return 0.0
        return self.tier2_hits / self.total_steps


class MetricsCollector:
    """Collects and manages simulation metrics throughout execution."""

    def __init__(self):
        self.metrics = SimulationMetrics()
        self.paths: Dict[int, Dict[str, Any]] = {}
        self.next_request_id = 0

    def record_step(self, match_type: str | None = None):
        """Record a traversal step execution."""
        self.metrics.total_steps += 1
        if match_type == 'Tier1':
            self.metrics.tier1_hits += 1
        elif match_type == 'Tier2':
            self.metrics.tier2_hits += 1
        else:
            self.metrics.misses += 1
            self.metrics.llm_calls += 1

    def record_execution_failure(self):
        """Record a failed strategy execution."""
        self.metrics.execution_failures += 1
    
    def record_sku_eviction(self, count: int = 1):
        """Record SKU evictions from cache cleanup."""
        self.metrics.sku_evictions += count
    
    def initialize_path(self, epoch: int, start_node: str, start_node_props: Dict[str, Any], goal: str, rubric: str) -> int:
        """Initialize a new traversal path tracking record."""
        request_id = self.next_request_id
        self.next_request_id += 1

        self.paths[request_id] = {
            "epoch": epoch,
            "start_node": start_node,
            "start_node_props": start_node_props,
            "goal": goal,
            "rubric": rubric,
            "steps": []
        }
        return request_id
    
    def record_path_step(
        self,
        request_id: int,
        tick: int,
        node_id: str,
        parent_node: str | None,
        parent_step_index: int | None,
        edge_label: str | None,
        structural_signature: str,
        goal: str,
        properties: Dict[str, Any],
        match_type: str | None,
        sku_id: str | None,
        decision: str | None,
    ):
        """Record a step in a traversal path."""
        if request_id not in self.paths:
            return
            
        self.paths[request_id]["steps"].append({
            "tick": tick,
            "node": node_id,
            "parent_node": parent_node,
            # For visualization only: explicit edge to previous step
            "parent_step_index": parent_step_index,
            "edge_label": edge_label,
            "s": structural_signature,
            "g": goal,
            "p": dict(properties),
            "match_type": match_type,
            "sku_id": sku_id,
            "decision": decision
        })
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all collected metrics."""
        return {
            "total_steps": self.metrics.total_steps,
            "llm_calls": self.metrics.llm_calls,
            "tier1_hits": self.metrics.tier1_hits,
            "tier2_hits": self.metrics.tier2_hits,
            "misses": self.metrics.misses,
            "execution_failures": self.metrics.execution_failures,
            "sku_evictions": self.metrics.sku_evictions,
            "hit_rate": self.metrics.hit_rate,
        }
    
    def print_summary(self):
        """Print a formatted summary of simulation metrics."""
        print("\n=== Simulation Results Analysis ===")
        print(f"Total Steps: {self.metrics.total_steps}")
        print(f"LLM Calls: {self.metrics.llm_calls}")
        print(f"Tier 1 Hits (Logic): {self.metrics.tier1_hits}")
        print(f"Tier 2 Hits (Similarity): {self.metrics.tier2_hits}")
        print(f"Execution Failures: {self.metrics.execution_failures}")
        print(f"SKU Evictions: {self.metrics.sku_evictions}")
        print(f"Overall Hit Rate: {self.metrics.hit_rate:.2%}")
        print(f"Tier 1 Hit Rate: {self.metrics.tier1_hit_rate:.2%}")
        print(f"Tier 2 Hit Rate: {self.metrics.tier2_hit_rate:.2%}")
