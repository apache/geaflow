"""Utility functions for JSON parsing, similarity calculations, and mathematical operations."""

import json
import math
import re
from typing import Any
import uuid

import numpy as np

from casts.core.models import StrategyKnowledgeUnit


def cosine_similarity(vector1: np.ndarray, vector2: np.ndarray) -> float:
    """
    Calculate cosine similarity between two vectors.

    Args:
        vector1: First vector
        vector2: Second vector

    Returns:
        Cosine similarity score between 0 and 1
    """
    norm1 = np.linalg.norm(vector1)
    norm2 = np.linalg.norm(vector2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return np.dot(vector1, vector2) / (norm1 * norm2)


def calculate_dynamic_similarity_threshold(
    sku: StrategyKnowledgeUnit, kappa: float = 0.05, beta: float = 0.2
) -> float:
    """
    Calculate dynamic similarity threshold based on manifold density.

    Mathematical formula (see 数学建模.md Section 4.6.2, line 952):
        δ_sim(v) = 1 - κ / (σ_logic(v) · (1 + β · log(η(v))))

    Design properties:
        1. δ_sim(v) ∈ (0,1) and monotonically non-decreasing with η(v)
        2. Higher confidence η → higher threshold → stricter matching
        3. Higher logic_complexity σ → higher threshold → stricter matching

    **CRITICAL: Counter-intuitive κ behavior!**
        - Higher κ → LOWER threshold → MORE permissive (easier to match)
        - Lower κ → HIGHER threshold → MORE strict (harder to match)
        This is because: κ↑ → κ/(...)↑ → 1-(large)↓

    Behavior examples (from 数学建模.md line 983-985):
        - Head scenario (η=1000, σ=1, β=0.1, κ=0.01): δ_sim ≈ 0.998 (very strict)
        - Tail scenario (η=0.5, σ=1, β=0.1, κ=0.01): δ_sim ≈ 0.99 (relaxed)
        - Complex logic (η=1000, σ=5, β=0.1, κ=0.01): δ_sim ≈ 0.99 (strict)

    Args:
        sku: Strategy knowledge unit containing η (confidence_score) and
             σ_logic (logic_complexity)
        kappa: Base threshold parameter (κ).
               Counter-intuitively: Higher κ → easier matching!
        beta: Frequency sensitivity parameter (β). Higher → high-frequency SKUs
              require stricter matching.

    Returns:
        Dynamic similarity threshold value in (0, 1)
    """

    # Ensure log domain is valid (confidence_score >= 1)
    confidence_val = max(1.0, sku.confidence_score)
    denominator = sku.logic_complexity * (1 + beta * math.log(confidence_val))
    return 1.0 - (kappa / denominator)


def calculate_tier2_threshold(min_confidence: float, gamma: float = 2.0) -> float:
    """
    Calculate Tier 2 confidence threshold.

    Formula: tier2_threshold = gamma * min_confidence
    where gamma > 1 to ensure higher bar for similarity matching

    Args:
        min_confidence: Minimum confidence threshold for Tier 1
        gamma: Scaling factor (must be > 1)

    Returns:
        Tier 2 confidence threshold
    """
    return gamma * min_confidence


def parse_jsons(
    text: str,
    start_marker: str = r"```(?:json)?\s*",
    end_marker: str = "```",
    placeholder_start_marker: str = "__PAYLOAD_START__",
    placeholder_end_marker: str = "__PAYLOAD_END__",
) -> list[dict[str, Any] | json.JSONDecodeError]:
    """
    Extract and parse JSON objects enclosed within specified markers from a text string.

    This function is designed to robustly handle JSON content from LLMs. It finds
    content between `start_marker` and `end_marker`, cleans it, and parses it.

    Cleaning steps include:
    1. Comment Removal (`// ...`)
    2. Single-Quoted Key Fix (`'key':` -> `"key":`)
    3. Trailing Comma Removal
    4. Control Character and BOM Removal

    Automatic Placeholder Feature for Complex Content:
    This function includes a powerful "placeholder" mechanism to handle complex,
    multi-line string content (like code, HTML, or Markdown) without requiring the
    LLM to perform error-prone escaping. This feature is enabled by default.

    How it works:
    1. The parser scans the raw JSON string for blocks enclosed by
       `placeholder_start_marker` (default: `__PAYLOAD_START__`) and
       `placeholder_end_marker` (default: `__PAYLOAD_END__`).
    2. It extracts the raw content from within these markers and stores it.
    3. It replaces the entire block (including markers) with a unique, quoted
       placeholder string (e.g., `"__PLACEHOLDER_uuid__"`). This makes the surrounding
       JSON syntactically valid for parsing.
    4. It then proceeds with standard cleaning and parsing of the simplified JSON.
    5. After successful parsing, it finds the placeholder string in the resulting
       Python object and injects the original raw content back.

    Example:
        text = '{"code": __PAYLOAD_START__\nprint("hello")\n__PAYLOAD_END__}'
        parse_jsons(text, start_marker='{', end_marker='}')
        # Result: [{'code': '\nprint("hello")\n'}]

    Args:
        text: The text string containing JSON content
        start_marker: Regex pattern for the start of the JSON content
        end_marker: The marker for the end of the JSON content
        placeholder_start_marker: The start marker for the complex block
        placeholder_end_marker: The end marker for the complex block

    Returns:
        List of parsed JSON objects or json.JSONDecodeError instances
    """
    # Add re.MULTILINE flag to allow ^ to match start of lines
    json_pattern = f"{start_marker}(.*?){re.escape(end_marker)}"
    json_matches = re.finditer(json_pattern, text, re.DOTALL | re.MULTILINE)
    results: list[dict[str, Any] | json.JSONDecodeError] = []

    def _find_and_replace_placeholders(obj: Any, extracted_payloads: dict[str, str]) -> None:
        """Recursively find and replace placeholders in the object."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and value in extracted_payloads:
                    obj[key] = extracted_payloads[value]
                else:
                    _find_and_replace_placeholders(value, extracted_payloads)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str) and item in extracted_payloads:
                    obj[i] = extracted_payloads[item]
                else:
                    _find_and_replace_placeholders(item, extracted_payloads)

    def _replace_with_placeholder(m, extracted_payloads: dict[str, str]):
        raw_content = m.group(1)
        # Generate a unique placeholder for each match
        placeholder = f"__PLACEHOLDER_{uuid.uuid4().hex}__"
        extracted_payloads[placeholder] = raw_content
        # The replacement must be a valid JSON string value
        return f'"{placeholder}"'

    for match in json_matches:
        json_str = match.group(1).strip()

        extracted_payloads: dict[str, str] = {}

        use_placeholder_logic = placeholder_start_marker and placeholder_end_marker

        if use_placeholder_logic:
            placeholder_pattern = re.compile(
                f"{re.escape(placeholder_start_marker)}(.*?){re.escape(placeholder_end_marker)}",
                re.DOTALL,
            )

            # Replace all occurrences of the placeholder block
            json_str = placeholder_pattern.sub(
                lambda m, p=extracted_payloads: _replace_with_placeholder(m, p),
                json_str,
            )

        try:
            # Remove comments
            lines = json_str.splitlines()
            cleaned_lines = []
            for line in lines:
                stripped_line = line.strip()
                if stripped_line.startswith("//"):
                    continue
                in_quotes = False
                escaped = False
                comment_start_index = -1
                for i, char in enumerate(line):
                    if char == '"' and not escaped:
                        in_quotes = not in_quotes
                    elif char == "/" and not in_quotes:
                        if i + 1 < len(line) and line[i + 1] == "/":
                            comment_start_index = i
                            break
                    escaped = char == "\\" and not escaped
                if comment_start_index != -1:
                    cleaned_line = line[:comment_start_index].rstrip()
                else:
                    cleaned_line = line
                if cleaned_line.strip():
                    cleaned_lines.append(cleaned_line)
            json_str_no_comments = "\n".join(cleaned_lines)

            # Fix single-quoted keys
            json_str_fixed_keys = re.sub(
                r"(?<=[{,])(\s*)'([^']+)'(\s*:)", r'\1"\2"\3', json_str_no_comments
            )
            json_str_fixed_keys = re.sub(
                r"({)(\s*)'([^']+)'(\s*:)", r'\1\2"\3"\4', json_str_fixed_keys
            )

            # Fix trailing commas
            json_str_fixed_commas = re.sub(r",\s*(?=[\}\]])", "", json_str_fixed_keys)

            # Remove control characters and BOM
            json_str_cleaned_ctrl = re.sub(
                r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", json_str_fixed_commas
            )
            if json_str_cleaned_ctrl.startswith("\ufeff"):
                json_str_cleaned = json_str_cleaned_ctrl[1:]
            else:
                json_str_cleaned = json_str_cleaned_ctrl

            if not json_str_cleaned.strip():
                continue

            # Parse the cleaned JSON string
            parsed_json = json.loads(json_str_cleaned)

            # Post-processing to inject back the payloads
            if use_placeholder_logic and extracted_payloads:
                _find_and_replace_placeholders(parsed_json, extracted_payloads)

            results.append(parsed_json)
        except json.JSONDecodeError as e:
            results.append(e)

    return results
