"""LLM Oracle for generating Strategy Knowledge Units (SKUs)."""

import re
from typing import Any, Dict, List

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
        self.sku_counter = 0

        # Use the centralized configuration method

        if isinstance(config, DefaultConfiguration):
            llm_cfg = config.get_llm_config()
            api_key = llm_cfg["api_key"]
            endpoint = llm_cfg["endpoint"]
            model = llm_cfg["model"]
        else:
            # Fallback for other configuration types
            api_key = config.get_str("LLM_APIKEY", "")
            endpoint = config.get_str("LLM_ENDPOINT", "")
            model = config.get_str("LLM_MODEL_NAME", "")

        if not api_key or not endpoint:
            print("Warning: LLM API credentials not configured, using fallback responses")
            self.client = None
        else:
            self.client = AsyncOpenAI(api_key=api_key, base_url=endpoint)

        self.model = model

    # --- Unified parsing & validation of decision strings ---
    @staticmethod
    def _parse_and_validate_decision(
        decision: str,
        valid_labels: List[str],
        safe_properties: Dict[str, Any],
    ) -> str:
        """
        Validate decision string against whitelist of Gremlin steps.

        Allowed formats:
        - out('label'), inV(), bothE('label'), otherV()
        - has('prop','value'), dedup(), limit(10)
        - order().by('prop'), values('name')
        - stop
        """
        decision = decision.strip()

        # Simple steps without arguments
        if decision == "stop":
            return "stop"
        if decision in ("dedup()", "dedup"):
            return "dedup()"
        if decision in ("inV()", "inV"):
            return "inV()"
        if decision in ("outV()", "outV"):
            return "outV()"
        if decision in ("otherV()", "otherV"):
            return "otherV()"

        # Traversal steps with a label argument
        m = re.match(r"^(out|in|both|outE|inE|bothE)\('([^']+)'\)$", decision)
        if m:
            step, label = m.group(1), m.group(2)
            if label not in valid_labels:
                raise ValueError(f"Invalid edge label '{label}' for step {step}")
            return f"{step}('{label}')"

        # has('prop','value')
        m = re.match(r"^has\('([^']+)'\s*,\s*'([^']*)'\)$", decision)
        if m:
            prop, value = m.group(1), m.group(2)
            # Only use properties that exist in safe_properties
            if prop not in safe_properties:
                raise ValueError(f"Invalid has prop '{prop}' (not in safe_properties)")
            allowed_val = str(safe_properties[prop])
            if value != allowed_val:
                raise ValueError(
                    f"Invalid has value '{value}' for prop '{prop}', "
                    f"expected '{allowed_val}' from safe_properties"
                )
            return f"has('{prop}','{value}')"

        # values('prop') or values()
        m = re.match(r"^values\((?:'([^']*)')?\)$", decision)
        if m:
            prop = m.group(1)
            # prop can be None for values() or a string for values('prop')
            return f"values('{prop}')" if prop is not None else "values()"

        # order().by('prop') or order()
        m = re.match(r"^order\(\)\.by\('([^']*)'\)$", decision)
        if m:
            # Could validate prop, but for now we accept any string
            return decision
        if decision in ("order()", "order"):
            return "order()"

        # limit(n)
        m = re.match(r"^limit\((\d+)\)$", decision)
        if m:
            return decision

        raise ValueError(f"Unsupported decision format: {decision}")

    async def generate_sku(self, context: Context, schema: GraphSchema) -> StrategyKnowledgeUnit:
        """Generate a new Strategy Knowledge Unit based on the current context.

        Args:
            context: The current traversal context
            schema: Graph schema for validation
        """
        self.sku_counter += 1

        # Get current state and next step options from state machine
        current_state, next_step_options = GremlinStateMachine.get_state_and_options(
            context.structural_signature
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

        node_id = context.properties.get("id", "")
        valid_labels = schema.get_valid_edge_labels(node_id)
        if not valid_labels:
            valid_labels = list(schema.edge_labels)

        safe_properties = context.safe_properties
        options_str = "\n      - ".join(next_step_options)

        state_desc = "Unknown"
        if current_state == "V":
            state_desc = "Vertex"
        elif current_state == "E":
            state_desc = "Edge"
        elif current_state == "P":
            state_desc = "Property/Value"

        prompt = f"""You are implementing a CASTS strategy inside a graph traversal engine.

Mathematical model (do NOT change it):
- A runtime context is c = (s, p, g)
  * s : structural pattern signature (current traversal path), a string
  * p : current node properties, a dict WITHOUT id/uuid (pure state)
  * g : goal text, describes the user's intent

- A Strategy Knowledge Unit (SKU) is:
  SKU = (c_sku, d_template, rho, v_proto, eta, sigma_logic)
  where
    * c_sku = (s_sku, Φ, g_sku)
      - s_sku: must EXACTLY equal the current s
      - Φ: a boolean predicate over p, written as a Python lambda
      - g_sku: must EXACTLY equal the current g
    * d_template: one traversal step template
    * rho: schema fingerprint (use "schema_v1")
    * v_proto: embedding of p at SKU creation time (runtime will fill this)
    * eta: confidence score (runtime initializes to 1.0)
    * sigma_logic: intrinsic logic complexity (fields + nesting), small integer

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

SCHEMA CONSTRAINTS (CRITICAL - MUST FOLLOW):
- Available edge labels from this node: {", ".join(valid_labels)}
- **IMPORTANT**: You MUST ONLY use edge labels from the list above. Using any other label will cause validation failure.
- If the goal suggests a label not in the list, choose the closest match from available labels.
- For traversal steps (out/in/both), the label MUST be one of: {", ".join(valid_labels)}

You must also define a `predicate` (a Python lambda on properties `p`) and a `sigma_logic` score (1-3 for complexity).

High-level requirements:
1) The `predicate` Φ should be general yet meaningful (e.g., check type, category, status, or ranges). NEVER use `id` or `uuid`.
2) The `d_template` should reflect the goal `g` when possible.
   - "Find friends": prefer 'friend'/'related' labels.
   - "Recommend products": prefer 'supplies'/'manages' labels.
   - "Detect fraud": prefer 'knows' or filter by risk properties.
   - Use `has()` for filtering, `order().by()` for sorting, `limit()` for restricting results.
   - **CRITICAL**: Only use edge labels that are in the available list above.
3) `sigma_logic`: 1 for a simple check, 2 for 2-3 conditions, 3 for more complex logic.

Return ONLY valid JSON inside <output> tags. Example:
<output>
{{
  "decision": "out('related')",
  "predicate": "lambda x: x.get('type') == 'TypeA' and x.get('status') == 'active'",
  "sigma_logic": 2
}}
</output>
"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=200,
            )

            content = response.choices[0].message.content.strip()
            results = parse_jsons(content, start_marker=r"^\s*<output>\s*", end_marker=r"</output>")
            if not results:
                raise ValueError(
                    f"No valid JSON found in response\nmessage: {content}\nprompt: {prompt}"
                )

            result = results[0]
            raw_decision = result.get("decision", "stop")

            try:
                decision = LLMOracle._parse_and_validate_decision(
                    raw_decision, valid_labels=valid_labels, safe_properties=safe_properties
                )

                decision_base = decision.split("(")[0].split(".")[0]
                allowed_bases = [opt.split("(")[0].split(".")[0] for opt in next_step_options]
                if decision_base not in allowed_bases:
                    raise ValueError(
                        f"Decision '{decision}' is not a valid next step from state '{current_state}'"
                    )

            except Exception as e:
                print(f"Decision validation failed: {e}, using fallback")
                raise

            try:
                predicate_code = result.get("predicate", "lambda x: True")
                predicate = eval(predicate_code)
                if not callable(predicate):
                    raise ValueError("Predicate not callable")
                _ = predicate(safe_properties)
            except Exception as e:
                print(f"Predicate validation failed: {e}, using default")

                def predicate(x):
                    return True

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
                confidence_score=1.0,
                logic_complexity=sigma_val,
            )
        except Exception as e:
            print(f"LLM API error: {e}, using goal-aware fallback")
            return await self._fallback_generate_sku(context, schema)

    async def _fallback_generate_sku(
        self, context: Context, schema: GraphSchema
    ) -> StrategyKnowledgeUnit:
        """Enhanced fallback that considers the goal when LLM is unavailable.

        Args:
            context: The current traversal context
            schema: Graph schema for validation
        """
        properties = context.safe_properties
        structural_signature = context.structural_signature
        goal = context.goal

        node_type = properties.get("type", "")
        goal_lower = goal.lower()

        # Map goals to sensible defaults
        if "friend" in goal_lower:
            # "Logistics Partner" plays the old TypeB role (more social / connector)
            target_label = "friend" if node_type == "Logistics Partner" else "related"
        elif "connect" in goal_lower:
            target_label = "related"
        elif "product" in goal_lower or "recommend" in goal_lower:
            target_label = "supplies" if node_type == "TypeC" else "manages"
        elif "fraud" in goal_lower or "risk" in goal_lower:
            target_label = "knows"
        elif "communit" in goal_lower:
            target_label = "friend"
        else:
            target_label = "related"

        # FIX: Validate label exists for this node
        node_id = context.properties.get("id", "")
        available_labels = schema.get_valid_edge_labels(node_id)
        if target_label not in available_labels and available_labels:
            target_label = available_labels[0]  # Use first available

        # All predicate lambdas assume input is "properties without id"
        if node_type == "Retail SME":  # formerly TypeA
            decision = f"out('{target_label}')"
            predicate = lambda x: x.get("type") == "Retail SME"
            sigma = 1
        elif node_type == "Logistics Partner":  # formerly TypeB
            decision = f"out('{target_label}')"
            predicate = lambda x: x.get("type") == "Logistics Partner"
            sigma = 1
        elif node_type == "Enterprise Vendor":  # formerly TypeC
            decision = f"in('{target_label}')"
            predicate = lambda x: x.get("type") == "Enterprise Vendor"
            sigma = 1
        else:
            decision = "stop"
            age = properties.get("age", 0)
            status = properties.get("status", "inactive")

            if age > 30:
                predicate = lambda x: x.get("age", 0) > 30
            else:
                predicate = lambda x: x.get("age", 0) <= 30

            if status == "active":
                base_pred = predicate
                predicate = lambda x: base_pred(x) and x.get("status") == "active"
                decision = f"out('{target_label}')"

            sigma = 2

        property_vector = await self.embed_service.embed_properties(properties)
        return StrategyKnowledgeUnit(
            id=f"SKU_{self.sku_counter}",
            structural_signature=structural_signature,
            goal_template=goal,
            predicate=predicate,
            property_vector=property_vector,
            decision_template=decision,
            schema_fingerprint="schema_v1",
            confidence_score=1.0,
            logic_complexity=sigma,
        )
