"""Gremlin traversal state machine for validating graph traversal steps."""

import re
from typing import List, Tuple

# Gremlin Step State Machine
# Defines valid transitions between step types (V: Vertex, E: Edge, P: Property)
GREMLIN_STEP_STATE_MACHINE = {
    # State: current element is a Vertex
    "V": {
        "options": [
            "out('label')", "in('label')", "both('label')",
            "outE('label')", "inE('label')", "bothE('label')",
            "has('prop','value')", "dedup()", "order().by('prop')", "limit(n)", "values('prop')",
            "stop"
        ],
        "transitions": {
            "out": "V", "in": "V", "both": "V",
            "outE": "E", "inE": "E", "bothE": "E",
            "has": "V", "dedup": "V", "order": "V", "limit": "V",
            "values": "P",
            "stop": "END"
        },
    },
    # State: current element is an Edge
    "E": {
        "options": [
            "inV()", "outV()", "otherV()",
            "has('prop','value')", "dedup()", "order().by('prop')", "limit(n)", "values('prop')",
            "stop"
        ],
        "transitions": {
            "inV": "V", "outV": "V", "otherV": "V",
            "has": "E", "dedup": "E", "order": "E", "limit": "E",
            "values": "P",
            "stop": "END"
        },
    },
    # State: current element is a Property/Value
    "P": {
        "options": ["order()", "limit(n)", "dedup()", "stop"],
        "transitions": {
            "order": "P", "limit": "P", "dedup": "P",
            "stop": "END"
        },
    },
    "END": {"options": [], "transitions": {}},
}


class GremlinStateMachine:
    """State machine for validating Gremlin traversal steps and determining next valid options."""

    @staticmethod
    def get_state_and_options(structural_signature: str) -> Tuple[str, List[str]]:
        """
        Parse traversal signature to determine current state (V, E, or P) and return valid next steps.
        
        Args:
            structural_signature: Current traversal path (e.g., "V().out().in()")
            
        Returns:
            Tuple of (current_state, list_of_valid_next_steps)
        """
        # Special case: initial state or empty
        if not structural_signature or structural_signature == "V()":
            return "V", GREMLIN_STEP_STATE_MACHINE["V"]["options"]
        
        state = "V"  # Assume starting from a Vertex context
        
        # Remove the prefix "V()" if it exists to get just the steps
        steps_part = structural_signature
        if steps_part.startswith("V()"):
            steps_part = steps_part[3:]  # Remove "V()"
        
        # Extract step names like 'out', 'inE', 'has', 'dedup', 'values'
        steps = re.findall(r'(\w+)(?=\()', steps_part)

        for step in steps:
            if state not in GREMLIN_STEP_STATE_MACHINE:
                state = "END"
                break

            transitions = GREMLIN_STEP_STATE_MACHINE[state]["transitions"]
            if step in transitions:
                state = transitions[step]
            else:
                # Unrecognized step in the current state, terminate
                state = "END"
                break

        # 'stop' is a terminal step that can appear without parentheses
        if "stop" in structural_signature:
            state = "END"

        if state in GREMLIN_STEP_STATE_MACHINE:
            return state, GREMLIN_STEP_STATE_MACHINE[state]["options"]
        
        return "END", []
