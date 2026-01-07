"""Gremlin traversal state machine for validating graph traversal steps."""

import re
from typing import Dict, List, Tuple

from casts.core.interfaces import GraphSchema

# Gremlin Step State Machine
# Defines valid transitions between step types (V: Vertex, E: Edge, P: Property)
GREMLIN_STEP_STATE_MACHINE: Dict[str, Dict[str, list[str] | Dict[str, str]]] = {
    # State: current element is a Vertex
    "V": {
        "options": [
            "out('label')",
            "in('label')",
            "both('label')",
            "outE('label')",
            "inE('label')",
            "bothE('label')",
            "has('prop','value')",
            "dedup()",
            "order().by('prop')",
            "limit(n)",
            "values('prop')",
            "stop",
        ],
        "transitions": {
            "out": "V",
            "in": "V",
            "both": "V",
            "outE": "E",
            "inE": "E",
            "bothE": "E",
            "has": "V",
            "dedup": "V",
            "order": "V",
            "limit": "V",
            "values": "P",
            "stop": "END",
        },
    },
    # State: current element is an Edge
    "E": {
        "options": [
            "inV()",
            "outV()",
            "otherV()",
            "has('prop','value')",
            "dedup()",
            "order().by('prop')",
            "limit(n)",
            "values('prop')",
            "stop",
        ],
        "transitions": {
            "inV": "V",
            "outV": "V",
            "otherV": "V",
            "has": "E",
            "dedup": "E",
            "order": "E",
            "limit": "E",
            "values": "P",
            "stop": "END",
        },
    },
    # State: current element is a Property/Value
    "P": {
        "options": ["order()", "limit(n)", "dedup()", "stop"],
        "transitions": {
            "order": "P",
            "limit": "P",
            "dedup": "P",
            "stop": "END",
        },
    },
    "END": {"options": [], "transitions": {}},
}


class GremlinStateMachine:
    """State machine for validating Gremlin traversal steps and determining next valid options."""

    @staticmethod
    def get_state_and_options(
        structural_signature: str, graph_schema: GraphSchema, node_id: str
    ) -> Tuple[str, List[str]]:
        """
        Parse traversal signature to determine current state (V, E, or P) and return
        valid next steps.

        Args:
            structural_signature: Current traversal path (e.g., "V().out().in()").
            graph_schema: The schema of the graph.
            node_id: The ID of the current node.

        Returns:
            Tuple of (current_state, list_of_valid_next_steps)
        """
        # Special case: initial state or empty
        if not structural_signature or structural_signature == "V()":
            state = "V"
        else:
            state = "V"  # Assume starting from a Vertex context

            # Improved regex to handle nested parentheses and chained calls
            steps_part = structural_signature
            if steps_part.startswith("V()"):
                steps_part = steps_part[3:]

            # Regex to correctly parse steps like order().by('prop') and single steps
            step_patterns = re.findall(r"\.([a-zA-Z_][a-zA-Z0-9_]*)\(.*?\)", steps_part)

            for step in step_patterns:
                if state not in GREMLIN_STEP_STATE_MACHINE:
                    state = "END"
                    break

                transitions = GREMLIN_STEP_STATE_MACHINE[state]["transitions"]
                base_step = step.split("().")[0]  # Handle chained calls like order().by

                if base_step in transitions:
                    state = transitions[base_step]
                else:
                    state = "END"
                    break

            # 'stop' is a terminal step that can appear without parentheses
            if ".stop" in structural_signature or structural_signature.endswith("stop"):
                state = "END"

        if state not in GREMLIN_STEP_STATE_MACHINE:
            return "END", []

        options = GREMLIN_STEP_STATE_MACHINE[state]["options"]
        final_options = []

        # Get valid labels from the schema
        out_labels = graph_schema.get_valid_outgoing_edge_labels(node_id)
        in_labels = graph_schema.get_valid_incoming_edge_labels(node_id)

        for option in options:
            if "('label')" in option:
                if any(step in option for step in ["out", "outE"]):
                    final_options.extend(
                        [option.replace("'label'", f"'{label}'") for label in out_labels]
                    )
                elif any(step in option for step in ["in", "inE"]):
                    final_options.extend(
                        [option.replace("'label'", f"'{label}'") for label in in_labels]
                    )
                elif any(step in option for step in ["both", "bothE"]):
                    all_labels = sorted(set(out_labels + in_labels))
                    final_options.extend(
                        [option.replace("'label'", f"'{label}'") for label in all_labels]
                    )
            else:
                final_options.append(option)

        return state, final_options
