"""Path quality evaluator for CASTS simulation results.

Scoring is aligned to CASTS core goals:
- Query effectiveness: does the path help answer the goal?
- Strategy reusability: are SKU decisions cacheable and generalizable?
- Cache efficiency: do we get Tier1/Tier2 hits instead of LLM fallbacks?
- Decision consistency: coherent strategy patterns that can be reused safely.
- Information utility: useful node attributes surfaced by the traversal.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from casts.services.path_judge import PathJudge
from casts.utils.helpers import parse_jsons


@dataclass
class PathEvaluationScore:
    """Detailed scoring breakdown for a single path evaluation."""

    query_effectiveness_score: float = 0.0  # 0-35
    strategy_reusability_score: float = 0.0  # 0-25
    cache_hit_efficiency_score: float = 0.0  # 0-20
    decision_consistency_score: float = 0.0  # 0-15
    information_utility_score: float = 0.0  # 0-5
    total_score: float = 0.0
    grade: str = "F"
    explanation: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.total_score = (
            self.query_effectiveness_score
            + self.strategy_reusability_score
            + self.cache_hit_efficiency_score
            + self.decision_consistency_score
            + self.information_utility_score
        )
        if self.total_score >= 90:
            self.grade = "A"
        elif self.total_score >= 80:
            self.grade = "B"
        elif self.total_score >= 70:
            self.grade = "C"
        elif self.total_score >= 60:
            self.grade = "D"
        else:
            self.grade = "F"


class PathEvaluator:
    """Evaluates CASTS traversal paths with a cache-focused rubric.

    Args:
        llm_judge: Class instance (e.g., PathJudge) exposing ``judge(payload) -> float``
            in the 0-35 range. It provides the LLM-as-judge view for query-effectiveness.
    """

    def __init__(self, llm_judge: PathJudge) -> None:
        self.llm_judge = llm_judge
        self.last_goal = None
        self.last_rubric = None

    def evaluate_subgraph(
        self,
        path_steps: List[Dict[str, Any]],
        goal: str,
        rubric: str,
        start_node: str,
        start_node_props: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None,
    ) -> PathEvaluationScore:
        """
        Evaluate a traversal subgraph and return detailed scoring.
        """
        self.last_goal = goal
        self.last_rubric = rubric

        if not path_steps:
            return PathEvaluationScore(
                explanation="Empty path - no steps to evaluate",
                details={"note": "empty_path"},
            )

        # Reconstruct the subgraph tree for the LLM prompt
        subgraph_nodes = {
            -1: {"step": {"node": start_node, "p": start_node_props}, "children": []}
        }  # sentinel root
        for i, step in enumerate(path_steps):
            subgraph_nodes[i] = {"step": step, "children": []}

        for i, step in enumerate(path_steps):
            parent_idx = step.get("parent_step_index")
            if parent_idx is not None and parent_idx in subgraph_nodes:
                subgraph_nodes[parent_idx]["children"].append(i)
            elif parent_idx is None:
                subgraph_nodes[-1]["children"].append(i)

        # Collect data from the entire subgraph for scoring
        all_props = [start_node_props] + [step.get("p", {}) for step in path_steps]
        all_match_types = [
            str(step.get("match_type")) for step in path_steps if step.get("match_type")
        ]
        all_sku_ids = [str(step.get("sku_id")) for step in path_steps if step.get("sku_id")]
        all_decisions = [
            str(step.get("decision", "")) for step in path_steps if step.get("decision")
        ]

        query_score, query_detail = self._score_query_effectiveness(
            goal, rubric, subgraph_nodes, schema
        )
        reuse_score, reuse_detail = self._score_strategy_reusability(
            all_sku_ids, all_decisions, path_steps
        )
        cache_score, cache_detail = self._score_cache_efficiency(all_match_types)
        consistency_score, consistency_detail = self._score_decision_consistency(
            all_decisions, all_props
        )
        info_score, info_detail = self._score_information_utility(all_props)

        explanation = self._build_explanation(
            query_score,
            reuse_score,
            cache_score,
            consistency_score,
            info_score,
        )

        details = {
            "query": query_detail,
            "reusability": reuse_detail,
            "cache": cache_detail,
            "consistency": consistency_detail,
            "info": info_detail,
            "nodes": len(all_props),
            "edges": len(path_steps),
            "schema_provided": schema is not None,
        }

        return PathEvaluationScore(
            query_effectiveness_score=query_score,
            strategy_reusability_score=reuse_score,
            cache_hit_efficiency_score=cache_score,
            decision_consistency_score=consistency_score,
            information_utility_score=info_score,
            explanation=explanation,
            details=details,
        )

    def _render_subgraph_ascii(
        self, nodes: Dict, root_idx: int, prefix: str = "", is_last: bool = True
    ) -> str:
        """Render the subgraph as an ASCII tree."""

        tree_str = prefix
        if prefix:
            tree_str += "└── " if is_last else "├── "

        step = nodes[root_idx]["step"]

        node_id = step.get("node", "?")
        node_type = step.get("p", {}).get("type", "?")
        decision = step.get("decision", "terminate")
        edge_label = step.get("edge_label", "")

        if root_idx == -1:  # Sentinel root
            tree_str += f"START: {node_id} ({node_type})\n"
        else:
            tree_str += f"via '{edge_label}' -> {node_id} [{node_type}] | Decision: {decision}\n"

        children = nodes[root_idx]["children"]
        for i, child_idx in enumerate(children):
            new_prefix = prefix + ("    " if is_last else "│   ")
            tree_str += self._render_subgraph_ascii(
                nodes, child_idx, new_prefix, i == len(children) - 1
            )

        return tree_str

    def _score_query_effectiveness(
        self,
        goal: str,
        rubric: str,
        subgraph: Dict,  # Changed from edges and props
        schema: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """Score query effectiveness via LLM judge (0–35)."""

        detail: Dict[str, Any] = {}

        coverage_bonus = 5.0 if len(subgraph) > 1 else 0.0
        detail["coverage_bonus"] = coverage_bonus

        subgraph_ascii = self._render_subgraph_ascii(subgraph, -1)

        instructions = f"""You are a CASTS path judge. Your task is to assess how well a traversal *subgraph* helps answer a user goal in a property graph.

**Your evaluation MUST be based *only* on the following rubric. Ignore all other generic metrics.**

**EVALUATION RUBRIC:**
{rubric}

System constraints (IMPORTANT):
- The CASTS system explores a subgraph of possibilities. You must judge the quality of this entire exploration.
- Do NOT speculate about better unseen paths; score based solely on the given subgraph and schema.

Context to consider (do not modify):
- Goal: {goal}
- Schema summary: {schema}
- Traversal Subgraph (ASCII tree view):
{subgraph_ascii}

Output requirements (IMPORTANT):
- Your response MUST be a single JSON code block, like this:
```json
{{
    "reasoning": {{
        "notes": "<string>"
    }},
    "score": <number 0-35>
}}
```
- Do NOT include any text outside the ```json ... ``` block.
"""

        payload = {
            "goal": goal,
            "subgraph_ascii": subgraph_ascii,
            "schema": schema,
            "instructions": instructions,
        }

        raw_response = str(self.llm_judge.judge(payload))
        print(f"[debug] LLM Judge Raw Response:\n{raw_response}\n[\\debug]\n")

        parsed = parse_jsons(raw_response)
        llm_score: float = 0.0
        reasoning: Dict[str, Any] = {}

        if parsed:
            first = parsed[0]
            if isinstance(first, dict) and "score" in first:
                try:
                    llm_score = float(first.get("score", 0.0))
                except (TypeError, ValueError):
                    llm_score = 0.0
                reasoning = (
                    first.get("reasoning", {})
                    if isinstance(first.get("reasoning", {}), dict)
                    else {}
                )
        detail["llm_score"] = llm_score
        detail["llm_reasoning"] = reasoning

        score = min(35.0, max(0.0, llm_score) + coverage_bonus)
        return score, detail

    def _score_strategy_reusability(
        self, sku_ids: List[str], decisions: List[str], steps: List[Dict[str, Any]]
    ) -> Tuple[float, Dict[str, Any]]:
        score = 0.0
        detail: Dict[str, Any] = {}

        reuse_count = len(sku_ids) - len(set(sku_ids))
        reuse_score = min(10.0, max(0, reuse_count) * 2.5)
        score += reuse_score
        detail["sku_reuse_count"] = reuse_count

        pattern_score = 0.0
        if decisions:
            dominant = self._dominant_pattern_ratio(decisions)
            pattern_score = dominant * 10.0
            score += pattern_score
        detail["decision_pattern_score"] = pattern_score

        avg_depth = sum(len(step.get("s", "")) for step in steps) / len(steps)
        if avg_depth <= 30:
            depth_score = 5.0
        elif avg_depth <= 60:
            depth_score = 3.0
        else:
            depth_score = 1.0
        score += depth_score
        detail["depth_score"] = depth_score

        return min(25.0, score), detail

    def _score_cache_efficiency(self, match_types: List[str]) -> Tuple[float, Dict[str, Any]]:
        detail: Dict[str, Any] = {}
        total = len(match_types)
        if total == 0:
            return 0.0, {"note": "no_steps"}

        tier1 = sum(1 for m in match_types if m == "Tier1")
        tier2 = sum(1 for m in match_types if m == "Tier2")
        misses = sum(1 for m in match_types if m is None)

        tier1_score = (tier1 / total) * 12.0
        tier2_score = (tier2 / total) * 6.0
        miss_penalty = (misses / total) * 8.0

        score = tier1_score + tier2_score - miss_penalty
        score = max(0.0, min(20.0, score))

        detail.update(
            {
                "tier1": tier1,
                "tier2": tier2,
                "misses": misses,
                "tier1_score": tier1_score,
                "tier2_score": tier2_score,
                "miss_penalty": miss_penalty,
            }
        )
        return score, detail

    def _score_decision_consistency(
        self, decisions: List[str], props: List[Dict[str, Any]]
    ) -> Tuple[float, Dict[str, Any]]:
        score = 0.0
        detail: Dict[str, Any] = {}

        direction_score = 0.0
        if decisions:
            out_count = sum(1 for d in decisions if "out" in d.lower())
            in_count = sum(1 for d in decisions if "in" in d.lower())
            both_count = sum(1 for d in decisions if "both" in d.lower())
            total = len(decisions)
            dominant = max(out_count, in_count, both_count) / total
            direction_score = dominant * 6.0
            score += direction_score
        detail["direction_score"] = direction_score

        type_score = 0.0
        transitions = []
        for i in range(len(props) - 1):
            t1 = props[i].get("type", "?")
            t2 = props[i + 1].get("type", "?")
            transitions.append((t1, t2))
        unique_transitions = len(set(transitions)) if transitions else 0
        if unique_transitions <= 3:
            type_score = 5.0
        elif unique_transitions <= 6:
            type_score = 3.0
        else:
            type_score = 1.0
        score += type_score
        detail["type_transition_score"] = type_score

        variety_score = 0.0
        if decisions:
            unique_decisions = len(set(decisions))
            if unique_decisions == 1:
                variety_score = 1.0
            elif unique_decisions == 2:
                variety_score = 2.0
            else:
                variety_score = 4.0
            score += variety_score
        detail["variety_score"] = variety_score

        return min(15.0, score), detail

    def _score_information_utility(
        self, props: List[Dict[str, Any]]
    ) -> Tuple[float, Dict[str, Any]]:
        detail: Dict[str, Any] = {}
        if not props:
            return 0.0, {"note": "no_properties"}

        keys = set()
        non_null = 0
        total = 0
        for prop in props:
            keys.update(prop.keys())
            for value in prop.values():
                total += 1
                if value not in (None, "", "null"):
                    non_null += 1
        key_score = min(3.0, len(keys) * 0.3)
        density = non_null / total if total else 0.0
        density_score = density * 2.0
        score = key_score + density_score
        detail["key_count"] = len(keys)
        detail["density"] = density
        return min(5.0, score), detail

    def _build_explanation(
        self,
        query_score: float,
        reuse_score: float,
        cache_score: float,
        consistency_score: float,
        info_score: float,
    ) -> str:
        parts = []
        parts.append(
            f"Query effectiveness: {query_score:.1f}/35; "
            f"Strategy reusability: {reuse_score:.1f}/25; "
            f"Cache efficiency: {cache_score:.1f}/20; "
            f"Decision consistency: {consistency_score:.1f}/15; "
            f"Information utility: {info_score:.1f}/5."
        )
        if cache_score < 5:
            parts.append("Cache misses high; consider improving SKU coverage.")
        if reuse_score < 8:
            parts.append("Strategies not clearly reusable; stabilize decisions/skus.")
        if query_score < 15:
            parts.append("Path only weakly answers the goal; tighten goal alignment.")
        return " ".join(parts)

    def _dominant_pattern_ratio(self, decisions: List[str]) -> float:
        counts: Dict[str, int] = {}
        for decision in decisions:
            counts[decision] = counts.get(decision, 0) + 1
        dominant = max(counts.values()) if counts else 0
        return dominant / len(decisions) if decisions else 0.0


class BatchEvaluator:
    """Batch evaluator for analyzing multiple paths."""

    def __init__(self, path_evaluator: PathEvaluator) -> None:
        self.path_evaluator = path_evaluator

    def evaluate_batch(
        self,
        paths: Dict[int, Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[int, PathEvaluationScore]:
        """
        Evaluate a batch of paths and return their evaluation scores.
        """
        results: Dict[int, PathEvaluationScore] = {}
        for request_id, path_data in paths.items():
            score = self.path_evaluator.evaluate_subgraph(
                path_steps=path_data.get("steps", []),
                goal=path_data.get("goal", ""),
                rubric=path_data.get("rubric", ""),
                start_node=path_data.get("start_node", ""),
                start_node_props=path_data.get("start_node_props", {}),
                schema=path_data.get("schema", schema),
            )
            results[request_id] = score
        return results

    def print_batch_summary(self, results: Dict[int, PathEvaluationScore]) -> None:
        """
        Print a summary of evaluation results for a batch of paths.
        """
        if not results:
            print("  No paths to evaluate.")
            return

        # If only one result, print a detailed summary for it
        if len(results) == 1:
            request_id, score = next(iter(results.items()))
            goal = (
                self.path_evaluator.last_goal
                if hasattr(self.path_evaluator, "last_goal")
                else "N/A"
            )
            rubric = (
                self.path_evaluator.last_rubric
                if hasattr(self.path_evaluator, "last_rubric")
                else "N/A"
            )
            print(f"  - Goal: {goal}")
            print(f"  - Rubric: {rubric}")
            print(f"  - Result: Grade {score.grade} (Score: {score.total_score:.1f}/100)")
            if score.details.get("llm_reasoning") and score.details["llm_reasoning"].get("notes"):
                print(f"  - Judge's Note: {score.details['llm_reasoning']['notes']}")
            return

        scores = list(results.values())
        total_scores = [score.total_score for score in scores]
        avg_score = sum(total_scores) / len(total_scores)
        max_score = max(total_scores)
        min_score = min(total_scores)

        print("\n=== Path Quality Evaluation Summary ===")
        print(f"Total Paths Evaluated: {len(scores)}")
        print("Overall Scores:")
        print(f"  Average: {avg_score:.2f}/100")
        print(f"  Maximum: {max_score:.2f}/100")
        print(f"  Minimum: {min_score:.2f}/100")

        grade_counts: Dict[str, int] = {}
        for score in scores:
            grade_counts[score.grade] = grade_counts.get(score.grade, 0) + 1
        print("Grade Distribution:")
        for grade in ["A", "B", "C", "D", "F"]:
            count = grade_counts.get(grade, 0)
            pct = (count / len(scores)) * 100
            print(f"  {grade}: {count} ({pct:.1f}%)")

        print("Average Component Scores:")
        print(
            "  Query Effectiveness: "
            f"{sum(s.query_effectiveness_score for s in scores) / len(scores):.2f}/35"
        )
        print(
            "  Strategy Reusability: "
            f"{sum(s.strategy_reusability_score for s in scores) / len(scores):.2f}/25"
        )
        print(
            "  Cache Hit Efficiency: "
            f"{sum(s.cache_hit_efficiency_score for s in scores) / len(scores):.2f}/20"
        )
        print(
            "  Decision Consistency: "
            f"{sum(s.decision_consistency_score for s in scores) / len(scores):.2f}/15"
        )
        print(
            "  Information Utility: "
            f"{sum(s.information_utility_score for s in scores) / len(scores):.2f}/5"
        )

        sorted_results = sorted(results.items(), key=lambda item: item[1].total_score, reverse=True)
        print("\n=== Top 3 Paths ===")
        for i, (req_id, score) in enumerate(sorted_results[:3], 1):
            print(
                f"{i}. Request #{req_id} - "
                f"Score: {score.total_score:.2f}/100 (Grade: {score.grade})"
            )
            print(f"   {score.explanation}")

        if len(sorted_results) > 3:
            print("\n=== Bottom 3 Paths ===")
            for i, (req_id, score) in enumerate(sorted_results[-3:], 1):
                print(
                    f"{i}. Request #{req_id} - "
                    f"Score: {score.total_score:.2f}/100 (Grade: {score.grade})"
                )
                print(f"   {score.explanation}")
