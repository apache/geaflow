"""Gremlin traversal state machine for validating graph traversal steps."""

from dataclasses import dataclass
from typing import Literal, Sequence, TypedDict

from casts.core.interfaces import GraphSchema


GremlinState = Literal["V", "E", "P", "END"]


class GremlinStateDefinition(TypedDict):
    """Typed representation of a Gremlin state definition."""

    options: list[str]
    transitions: dict[str, GremlinState]


# Gremlin Step State Machine
# Defines valid transitions between step types (V: Vertex, E: Edge, P: Property)
GREMLIN_STEP_STATE_MACHINE: dict[GremlinState, GremlinStateDefinition] = {
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
            "simplePath()",
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
            "simplePath": "V",
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
            "simplePath()",
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
            "simplePath": "E",
            "order": "E",
            "limit": "E",
            "values": "P",
            "stop": "END",
        },
    },
    # State: current element is a Property/Value
    "P": {
        "options": ["order()", "limit(n)", "dedup()", "simplePath()", "stop"],
        "transitions": {
            "order": "P",
            "limit": "P",
            "dedup": "P",
            "simplePath": "P",
            "stop": "END",
        },
    },
    "END": {"options": [], "transitions": {}},
}

_MODIFIER_STEPS = {"by"}
_MODIFIER_COMPATIBILITY = {"by": {"order"}}


@dataclass(frozen=True)
class ParsedStep:
    """Parsed step representation for traversal signatures."""

    raw: str
    name: str


def _normalize_signature(signature: str) -> str:
    """Normalize a traversal signature by stripping the V() prefix and separators."""
    normalized = signature.strip()
    if not normalized or normalized == "V()":
        return ""

    if normalized.startswith("V()"):
        normalized = normalized[3:]
    elif normalized.startswith("V"):
        normalized = normalized[1:]

    return normalized.lstrip(".")


def _split_steps(signature: str) -> list[str]:
    """Split a traversal signature into raw step segments."""
    if not signature:
        return []

    steps: list[str] = []
    current: list[str] = []
    depth = 0

    for ch in signature:
        if ch == "." and depth == 0:
            if current:
                steps.append("".join(current))
                current = []
            continue

        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)

        current.append(ch)

    if current:
        steps.append("".join(current))

    return [step for step in steps if step]


def _extract_step_name(step: str) -> str:
    """Extract the primary step name from a step string."""
    head = step.split("(", 1)[0]
    if "." in head:
        return head.split(".", 1)[0]
    return head


def _combine_modifiers(steps: Sequence[str]) -> list[str]:
    """Combine modifier steps (e.g., order().by()) into a single step string."""
    combined: list[str] = []
    for step in steps:
        step_name = _extract_step_name(step)
        if step_name in _MODIFIER_STEPS and combined:
            previous_name = _extract_step_name(combined[-1])
            if previous_name in _MODIFIER_COMPATIBILITY.get(step_name, set()):
                combined[-1] = f"{combined[-1]}.{step}"
                continue
        combined.append(step)
    return combined


def _parse_traversal_signature(signature: str) -> list[ParsedStep]:
    """Parse traversal signature into steps with normalized names."""
    normalized = _normalize_signature(signature)
    raw_steps = _combine_modifiers(_split_steps(normalized))
    return [ParsedStep(raw=step, name=_extract_step_name(step)) for step in raw_steps]


class GremlinStateMachine:
    """State machine for validating Gremlin traversal steps and determining next valid options."""

    @staticmethod
    def parse_traversal_signature(structural_signature: str) -> list[str]:
        """Parse traversal signature into decision steps for display or history."""
        return [step.raw for step in _parse_traversal_signature(structural_signature)]

    @staticmethod
    def get_state_and_options(
        structural_signature: str, graph_schema: GraphSchema, node_id: str
    ) -> tuple[GremlinState, list[str]]:
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

            last_primary_step: str | None = None
            for step in _parse_traversal_signature(structural_signature):
                if state not in GREMLIN_STEP_STATE_MACHINE:
                    state = "END"
                    break

                if step.name == "stop":
                    state = "END"
                    break

                if step.name in _MODIFIER_STEPS:
                    if last_primary_step and last_primary_step in _MODIFIER_COMPATIBILITY.get(
                        step.name, set()
                    ):
                        continue
                    state = "END"
                    break

                transitions = GREMLIN_STEP_STATE_MACHINE[state]["transitions"]
                if step.name in transitions:
                    state = transitions[step.name]
                    last_primary_step = step.name
                else:
                    state = "END"
                    break

        if state not in GREMLIN_STEP_STATE_MACHINE:
            return "END", []

        options = GREMLIN_STEP_STATE_MACHINE[state]["options"]
        final_options = []

        # Get valid labels from the schema
        out_labels = sorted(graph_schema.get_valid_outgoing_edge_labels(node_id))
        in_labels = sorted(graph_schema.get_valid_incoming_edge_labels(node_id))

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
