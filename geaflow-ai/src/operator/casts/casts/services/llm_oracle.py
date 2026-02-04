"""LLM Oracle for generating Strategy Knowledge Units (SKUs)."""

from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
import re
from typing import Any

from openai import AsyncOpenAI

from casts.core.config import DefaultConfiguration
from casts.core.gremlin_state import GremlinStateMachine
from casts.core.interfaces import Configuration, GraphSchema
from casts.core.models import Context, StrategyKnowledgeUnit
from casts.services.embedding import EmbeddingService
from casts.utils.helpers import parse_jsons


class LLMOracle:
    """Real LLM Oracle using OpenRouter API for generating traversal strategies."""

    def __init__(self, embed_service: EmbeddingService, config: Configuration):
        """Initialize LLM Oracle with configuration.

        Args:
            embed_service: Embedding service instance
            config: Configuration object containing API settings
        """
        self.embed_service = embed_service
        self.config = config
        self.sku_counter = 0

        # Setup debug log file
        # Use path relative to CASTS project root
        log_dir = Path(__file__).parent.parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.debug_log_file = log_dir / f"llm_oracle_debug_{timestamp}.txt"

        # Use the centralized configuration method

        if isinstance(config, DefaultConfiguration):
            llm_cfg = config.get_llm_config()
            api_key = llm_cfg["api_key"]
            endpoint = llm_cfg["endpoint"]
            model = llm_cfg["model"]
        else:
            # Fallback for other configuration types
            api_key = config.get_str("LLM_APIKEY")
            endpoint = config.get_str("LLM_ENDPOINT")
            model = config.get_str("LLM_MODEL_NAME")

        if not api_key or not endpoint:
            self._write_debug(
                "Warning: LLM API credentials not configured, using fallback responses"
            )
            self.client = None
        else:
            self.client = AsyncOpenAI(api_key=api_key, base_url=endpoint)

        self.model = model

    def _write_debug(self, message: str) -> None:
        """Write debug message to log file.

        Args:
            message: Debug message to write
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.debug_log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")

    @staticmethod
    def _extract_recent_decisions(signature: str, depth: int = 3) -> list[str]:
        """Extract the most recent N decisions from a traversal signature.

        Args:
            signature: The traversal signature (e.g., "V().out('friend').has('type','Person')")
            depth: Number of recent decisions to extract (default: 3)

        Returns:
            List of recent decision strings (e.g., ["out('friend')", "has('type','Person')"])
        """
        decisions = GremlinStateMachine.parse_traversal_signature(signature)
        return decisions[-depth:] if len(decisions) > depth else decisions

    @staticmethod
    def _parse_and_validate_decision(
        decision: str,
        valid_options: list[str],
        safe_properties: dict[str, Any],
    ) -> str:
        """
        Validate the LLM's decision against the list of valid options provided by the state machine.

        Args:
            decision: The decision string from the LLM.
            valid_options: A list of valid, fully-formed Gremlin steps.
            safe_properties: A dictionary of the current node's safe properties.

        Returns:
            The validated decision string.

        Raises:
            ValueError: If the decision is not in the list of valid options.
        """
        decision = decision.strip()

        if decision in valid_options:
            # Additionally, validate `has` step values against current properties
            if decision.startswith("has("):
                m = re.match(r"^has\('([^']+)'\s*,\s*'([^']*)'\)$", decision)
                if m:
                    prop, value = m.group(1), m.group(2)
                    if prop not in safe_properties:
                        raise ValueError(f"Invalid has prop '{prop}' (not in safe_properties)")
                    allowed_val = str(safe_properties[prop])
                    if value != allowed_val:
                        raise ValueError(
                            f"Invalid has value '{value}' for prop '{prop}', "
                            f"expected '{allowed_val}' from safe_properties"
                        )
            return decision

        raise ValueError(f"Decision '{decision}' is not in the list of valid options.")

    async def generate_sku(self, context: Context, schema: GraphSchema) -> StrategyKnowledgeUnit:
        """Generate a new Strategy Knowledge Unit based on the current context.

        Args:
            context: The current traversal context
            schema: Graph schema for validation
        """
        self.sku_counter += 1

        # Get current state and next step options from state machine
        node_id = context.properties.get("id", "")
        current_state, next_step_options = GremlinStateMachine.get_state_and_options(
            context.structural_signature, schema, node_id
        )

        # If no more steps are possible, force stop
        if not next_step_options or current_state == "END":
            property_vector = await self.embed_service.embed_properties(context.safe_properties)
            return StrategyKnowledgeUnit(
                id=f"SKU_{self.sku_counter}",
                structural_signature=context.structural_signature,
                predicate=lambda x: True,
                goal_template=context.goal,
                decision_template="stop",
                schema_fingerprint="schema_v1",
                property_vector=property_vector,
                confidence_score=1.0,
                logic_complexity=1,
            )

        safe_properties = context.safe_properties
        options_str = "\n      - ".join(next_step_options)

        state_desc = "Unknown"
        if current_state == "V":
            state_desc = "Vertex"
        elif current_state == "E":
            state_desc = "Edge"
        elif current_state == "P":
            state_desc = "Property/Value"

        # Extract recent decision history for context
        recent_decisions = self._extract_recent_decisions(context.structural_signature, depth=3)
        if recent_decisions:
            history_str = "\n".join([f"  {i + 1}. {dec}" for i, dec in enumerate(recent_decisions)])
            history_section = f"""
Recent decision history (last {len(recent_decisions)} steps):
{history_str}
"""
        else:
            history_section = "Recent decision history: (no previous steps, starting fresh)\n"

        def _format_list(values: list[str], max_items: int = 12) -> str:
            if len(values) <= max_items:
                return ", ".join(values) if values else "none"
            head = ", ".join(values[:max_items])
            return f"{head}, ... (+{len(values) - max_items} more)"

        node_type = safe_properties.get("type") or context.properties.get("type")
        node_schema = schema.get_node_schema(str(node_type)) if node_type else {}
        outgoing_labels = schema.get_valid_outgoing_edge_labels(node_id)
        incoming_labels = schema.get_valid_incoming_edge_labels(node_id)

        max_depth = self.config.get_int("SIMULATION_MAX_DEPTH")
        current_depth = len(
            GremlinStateMachine.parse_traversal_signature(context.structural_signature)
        )
        remaining_steps = max(0, max_depth - current_depth)

        schema_summary = f"""Schema summary (context only):
- Node types: {_format_list(sorted(schema.node_types))}
- Edge labels: {_format_list(sorted(schema.edge_labels))}
- Current node type: {node_type if node_type else "unknown"}
- Current node outgoing labels: {_format_list(sorted(outgoing_labels))}
- Current node incoming labels: {_format_list(sorted(incoming_labels))}
- Current node type properties: {node_schema.get("properties", {})}
"""

        has_simple_path = "simplePath()" in context.structural_signature
        simple_path_status = (
            "Already using simplePath()" if has_simple_path else "Not using simplePath()"
        )

        prompt = f"""You are implementing a CASTS strategy inside a graph traversal engine.

Mathematical model (do NOT change it):
- A runtime context is c = (s, p, g)
  * s : structural pattern signature (current traversal path), a string
  * p : current node properties, a dict WITHOUT id/uuid (pure state)
  * g : goal text, describes the user's intent

{history_section}
Iteration model (important):
- This is a multi-step, iterative process: you will be called repeatedly until a depth budget is reached.
- You are NOT expected to solve the goal in one step; choose a step that moves toward the goal over 2-4 hops.
- Current depth: {current_depth} / max depth: {max_depth} (remaining steps: {remaining_steps})
- Avoid "safe but useless" choices (e.g. stopping too early) when meaningful progress is available.

About simplePath():
- `simplePath()` is a FILTER, not a movement. It helps avoid cycles, but it does not expand to new nodes.
- Prefer expanding along goal-aligned edges first; add `simplePath()` after you have at least one traversal edge
  when cycles become a concern.
- Current path signature: {context.structural_signature}
- simplePath status: {simple_path_status}

{schema_summary}
Reminder: Schema is provided for context only. You MUST choose from the valid next steps list
below. Schema does not expand the allowed actions.

Your task in THIS CALL:
- Given current c = (s, p, g) below, you must propose ONE new SKU:
  * s_sku = current s
  * g_sku = current g
  * Φ(p): a lambda over SAFE properties only (NO id/uuid)
  * d_template: exactly ONE of the following valid next steps based on the current state:
      - {options_str}

Current context c:
- s      = {context.structural_signature}
- (derived) current traversal state = {current_state} (on a {state_desc})
- p      = {safe_properties}
- g      = {context.goal}

You must also define a `predicate` (a Python lambda on properties `p`) and a `sigma_logic` score (1-3 for complexity).

High-level requirements:
1) The `predicate` Φ should be general yet meaningful (e.g., check type, category, status, or ranges). NEVER use `id` or `uuid`.
2) The `d_template` should reflect the goal `g` when possible.
3) This is iterative: prefer actions that unlock goal-relevant node types and relations within the remaining depth.
4) `sigma_logic`: 1 for a simple check, 2 for 2-3 conditions, 3 for more complex logic.
5) Choose `stop` ONLY if there is no useful progress you can make with the remaining depth.
6) To stay general across schemas, do not hardcode domain assumptions; choose steps based on the goal text and the provided valid options.

Return ONLY valid JSON inside <output> tags. Example:
<output>
{{
  "reasoning": "Goal requires finding suppliers without revisiting nodes, so using simplePath()",
  "decision": "simplePath()",
  "predicate": "lambda x: x.get('type') == 'TypeA'",
  "sigma_logic": 1
}}
</output>
"""  # noqa: E501
        last_error = "Unknown error"
        prompt_with_feedback = prompt

        for attempt in range(2):  # Allow one retry
            # Augment prompt on the second attempt
            if attempt > 0:
                prompt_with_feedback = (
                    prompt + f'\n\nYour previous decision was invalid. Error: "{last_error}". '
                    f"Please review the valid options and provide a new, valid decision."
                )

            try:
                self._write_debug(
                    f"LLM Oracle Prompt (Attempt {attempt + 1}):\n{prompt_with_feedback}\n"
                    "--- End of Prompt ---\n"
                )
                if not self.client:
                    raise ValueError("LLM client not available.")

                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt_with_feedback}],
                    temperature=0.1 + (attempt * 0.2),  # Increase temperature on retry
                    max_tokens=200,
                )

                content = response.choices[0].message.content
                if not content:
                    raise ValueError("LLM response content is empty.")

                results = parse_jsons(
                    content.strip(), start_marker=r"^\s*<output>\s*", end_marker=r"</output>"
                )
                if not results:
                    raise ValueError(f"No valid JSON found in response on attempt {attempt + 1}")

                result = results[0]
                if isinstance(result, JSONDecodeError):
                    raise ValueError(f"JSON decoding failed on attempt {attempt + 1}: {result}")
                self._write_debug(
                    f"LLM Oracle Response (Attempt {attempt + 1}):\n{result}\n"
                    "--- End of Response ---\n"
                )

                raw_decision = result.get("decision", "stop")
                decision = LLMOracle._parse_and_validate_decision(
                    raw_decision, valid_options=next_step_options, safe_properties=safe_properties
                )

                # --- Success Path ---
                # If validation succeeds, construct and return the SKU immediately
                def _default_predicate(_: dict[str, Any]) -> bool:
                    return True

                try:
                    predicate_code = result.get("predicate", "lambda x: True")
                    predicate = eval(predicate_code)
                    if not callable(predicate):
                        predicate = _default_predicate
                    _ = predicate(safe_properties)  # Test call
                except Exception:
                    predicate = _default_predicate

                property_vector = await self.embed_service.embed_properties(safe_properties)
                sigma_val = result.get("sigma_logic", 1)
                if sigma_val not in (1, 2, 3):
                    sigma_val = 2

                return StrategyKnowledgeUnit(
                    id=f"SKU_{self.sku_counter}",
                    structural_signature=context.structural_signature,
                    predicate=predicate,
                    goal_template=context.goal,
                    property_vector=property_vector,
                    decision_template=decision,
                    schema_fingerprint="schema_v1",
                    confidence_score=1.0,  # Start with high confidence
                    logic_complexity=sigma_val,
                )

            except (ValueError, AttributeError, TypeError) as e:
                last_error = str(e)
                self._write_debug(f"LLM Oracle Attempt {attempt + 1} failed: {last_error}")
                continue  # Go to the next attempt

        # --- Fallback Path ---
        # If the loop completes without returning, all attempts have failed.
        self._write_debug(
            f"All LLM attempts failed. Last error: {last_error}. Falling back to 'stop'."
        )
        property_vector = await self.embed_service.embed_properties(safe_properties)
        return StrategyKnowledgeUnit(
            id=f"SKU_{self.sku_counter}",
            structural_signature=context.structural_signature,
            predicate=lambda x: True,
            goal_template=context.goal,
            decision_template="stop",
            schema_fingerprint="schema_v1",
            property_vector=property_vector,
            confidence_score=1.0,
            logic_complexity=1,
        )

    async def recommend_starting_node_types(
        self,
        goal: str,
        available_node_types: set[str],
        max_recommendations: int = 3,
    ) -> list[str]:
        """Recommend suitable starting node types for a given goal.

        Uses LLM to analyze the goal text and recommend 1-3 node types
        that would be most appropriate as starting points for traversal.

        Args:
            goal: The traversal goal text
            available_node_types: Set of available node types from the schema
            max_recommendations: Maximum number of node types to recommend (default: 3)

        Returns:
            List of recommended node type strings (1-3 types).
            Returns empty list if LLM fails or no suitable types found.
        """
        if not available_node_types:
            self._write_debug("No available node types, returning empty list")
            return []

        # Convert set to sorted list for consistent ordering
        node_types_list = sorted(available_node_types)
        node_types_str = ", ".join(f'"{nt}"' for nt in node_types_list)

        prompt = f"""You are analyzing a graph traversal goal to recommend starting node types.

Goal: "{goal}"

Available node types: [{node_types_str}]

Recommend 1-{
            max_recommendations
        } node types that would be most suitable as starting points for this traversal goal.
Consider which node types are most likely to:
1. Have connections relevant to the goal
2. Be central to the graph topology
3. Enable meaningful exploration toward the goal's objective

Return ONLY a JSON array of node type strings (no explanations).

Example outputs:
["Person", "Company"]
["Account"]
["Person", "Company", "Loan"]

Your response (JSON array only, using ```json), for example:
```json
["Company"]
```
"""  # noqa: E501

        try:
            self._write_debug(
                f"Node Type Recommendation Prompt:\n{prompt}\n--- End of Prompt ---\n"
            )

            if not self.client:
                self._write_debug(
                    "LLM client not available, falling back to all node types"
                )
                # Fallback: return all types if LLM unavailable
                return node_types_list[:max_recommendations]

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,  # Moderate creativity
                max_tokens=100,
            )

            content = response.choices[0].message.content
            if not content:
                self._write_debug("LLM response content is empty, falling back")
                return []

            self._write_debug(f"LLM Raw Response:\n{content}\n--- End of Response ---\n")

            # Use parse_jsons to robustly extract JSON from response
            results = parse_jsons(content.strip())

            if not results:
                self._write_debug("No valid JSON found in response")
                return []

            result = results[0]
            if isinstance(result, JSONDecodeError):
                self._write_debug(f"JSON decoding failed: {result}")
                return []

            # Result should be a list of strings
            if isinstance(result, list):
                # Filter to only valid node types and limit to max
                recommended = [
                    nt for nt in result
                    if isinstance(nt, str) and nt in available_node_types
                ][:max_recommendations]

                self._write_debug(
                    f"Successfully extracted {len(recommended)} node types: {recommended}"
                )
                return recommended
            else:
                self._write_debug(f"Unexpected result type: {type(result)}")
                return []

        except Exception as e:
            self._write_debug(f"Error in recommend_starting_node_types: {e}")
            return []
