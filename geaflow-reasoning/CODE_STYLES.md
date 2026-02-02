# CASTS Code Styles

This document records the current CASTS code conventions used in this repo.
Keep changes consistent with these rules unless there is a strong reason to deviate.

## Tooling

- Format/lint: `ruff` (line length: 100; formatter uses double quotes).
- Type check: `mypy`.
- Tests: `pytest`.

Common commands:

- `ruff check .`
- `mypy .`
- `pytest tests`

## Python Version

- Target runtime: Python 3.10+ (repo uses Python 3.11 in local runs).

## Formatting & Imports

- Keep lines ≤ 100 chars (ruff-enforced).
- Use `ruff format` output style (double quotes, standard indentation).
- Import order is ruff/isort-managed:
  1) stdlib
  2) third-party
  3) first-party (`casts.*`)
- Prefer explicit imports (`from typing import ...`) over `import typing as t`.

## Typing Rules

- Prefer `Optional[T]` over `T | None`.
- Do **not** add `from __future__ import annotations`.
- Prefer explicit container types (`List`, `Dict`, `Set`, `Tuple`) consistent with existing code.
- Avoid `Any` unless required by external I/O or generic containers; keep `Any` localized.
- Interfaces use ABCs/Protocols (`casts/core/interfaces.py`); concrete implementations live in `casts/*`.

## Naming

- Variables/functions: `snake_case`.
- Classes: `CapWords`.
- Constants: `UPPER_SNAKE_CASE`.
- Private methods/attrs: prefix with `_` (e.g., `_ensure_ready`, `self._node_types`).
- Use descriptive names (avoid single-letter names except for tight local scopes).

## Docstrings

- Module docstring at top of file (one paragraph summary).
- Public class/function/method docstrings are expected.
- Use a consistent structure:
  - Short summary line
  - Blank line
  - `Args:` / `Returns:` / `Raises:` as applicable
- Keep docstrings precise and aligned with behavior; avoid stale comments.

## Error Handling

- Prefer explicit exception types; avoid bare `except:`.
- Fallback paths are allowed but must be deterministic and simple (no noisy logging).
- When validating external/model outputs, validate early and fail clearly.

## Configuration

- Defaults must live in `casts/core/config.py` (`DefaultConfiguration`), not at call sites.
- Do not pass ad-hoc defaults to `config.get_int/get_float/get_bool/get_str` in production code.
  - Exception: tests may use mocks that accept a default parameter for compatibility.

## Clean Output / Logging

- Simulation output should be controlled via `verbose` flags (no unconditional spam).
- Avoid adding extra debug logs/guards for correctness fixes; keep code clean and direct.

## Generality (Non-cheating)

- CASTS should remain schema-agnostic.
- Avoid special-casing goals/benchmarks by injecting “goal-aligned” heuristics into core logic.
- Use only universally available signals (current node properties, schema constraints, valid options, depth budget).
