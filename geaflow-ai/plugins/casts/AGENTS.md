# CASTS Agent Instructions (geaflow-ai/plugins/casts)

This file defines CASTS plugin-local instructions for coding agents.

## Must-Read (Before You Change Code)

- Review and follow: `geaflow-ai/plugins/CODE_STYLES.md`
  - Treat it as the baseline contract for changes under `geaflow-ai/plugins/casts/`.
  - If you need to break a rule, document the reason in the PR/commit message.

## Repository Layout (What Goes Where)

- `core/`: deterministic cache + decision logic (no network calls required).
- `services/`: integration code (LLM / embedding / external I/O).
- `harness/`: offline simulation harness (data + executor + evaluator).
- `api/`: production-facing decision service (FastAPI).
  - Endpoint: `POST /casts/decision`
  - Safety: must degrade conservatively to `decision="stop"` on invalid input or upstream failures.
- `scripts/`: local developer scripts (e.g., smoke tests).
- `tests/`: pytest suite.

## Local Dev (Python 3.11 + uv)

We use a per-plugin venv in `.venv/` (gitignored) and a **no-activate** workflow.

One-time setup:

```bash
cd geaflow-ai/plugins/casts
[ -d .venv ] || python3.11 -m venv .venv
uv sync --extra dev
```

Run tests:

```bash
cd geaflow-ai/plugins/casts
uv run pytest -q
```

Run lint + type checks:

```bash
cd geaflow-ai/plugins/casts
uv run ruff format --check .
uv run ruff check .
uv run mypy -p api -p core -p services -p harness
```

## Run The Service (FastAPI)

```bash
cd geaflow-ai/plugins/casts
uv sync --extra dev
uv run uvicorn api.app:app --host 127.0.0.1 --port 5001
```

One-click smoke:

```bash
cd geaflow-ai/plugins/casts
./scripts/smoke.sh
```

## Safety Defaults

- Never enable evaluating LLM-provided predicates in production:
  - `LLM_ORACLE_ENABLE_PREDICATE_EVAL` must remain `False` by default.
- `scope` is a hard boundary:
  - requests with empty scope must be rejected (or conservatively downgraded).
  - CASTS service additionally requires `scope.run_id` for cache isolation; if missing, it downgrades to `stop`.
