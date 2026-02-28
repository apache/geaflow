# CASTS (Context-Aware Strategy Cache System)

CASTS is a strategy cache for graph traversal systems. It learns, stores, and
retrieves traversal decisions (strategies) based on structural signatures,
node properties, and goals, with optional LLM-driven guidance.

## Name Origin

CASTS stands for **Context-Aware Strategy Cache System**.

## Design Goals

- Cache traversal strategies (SKUs) to reduce repeated LLM calls.
- Separate schema metadata from execution logic.
- Support both synthetic and real-world graph data (in the Python harness).
- Keep the core cache logic deterministic and testable.

## Two Modes: Production vs. Python Harness

CASTS is designed so that **decisioning** and **execution** are separate concerns.

### Production Mode (GeaFlow Java Integration)

In production, the **GeaFlow Java data plane executes traversal** and CASTS is
used as a *decision service*:

- Python returns a **single next-step Gremlin-style step string** (e.g. `out('friend')`, `inV()`, `stop`).
- Python does **not** execute multi-hop traversal against graph data.
- Java (`CastsOperator`) executes the step, expands the subgraph, and repeats until
  `stop` or `maxDepth`.

Key integration files in this repo:

- Java executor/operator: `geaflow-ai/src/main/java/org/apache/geaflow/ai/casts/CastsOperator.java`
- Python decision service: `geaflow-ai/plugins/casts/api/app.py` (`POST /casts/decision`)

### Python Harness Mode (Offline Simulation)

The CASTS repo also includes a **Python-only harness** for experiments:

- It implements its own in-memory data source + traversal executor for evaluation.
- It is intended for offline testing, hit-rate studies, and algorithm iteration.
- It can be misleading if read as “production execution”; treat it as a harness.

## Module Layout

- `core`: decision core (cache, models, GremlinStateMachine, validation)
- `services`: embedding + LLM integrations used by decisioning
- `harness`: Python-only data + simulation + executor (not production execution)
- `tests`: unit and integration tests

## Repository Placement

This module is intended to live under `geaflow-ai/plugins/casts` as a standalone
plugin, with the Python package located at the module root.

## Configuration (Required)

These env vars are required to run CASTS with real embedding + LLM services
(typically used by the Python harness). Missing values raise a `ValueError`
when those services are instantiated:

- `EMBEDDING_ENDPOINT`
- `EMBEDDING_APIKEY`
- `EMBEDDING_MODEL`
- `LLM_ENDPOINT`
- `LLM_APIKEY`
- `LLM_MODEL`

You can use a local `.env` file for development. The code does not provide
automatic fallbacks for missing credentials.

## Real Data Loading

The default harness loader reads CSV files from:

- `harness/data/real_graph_data` (repo default), or
- `real_graph_data` (alternate path), or
- a configured `GraphGeneratorConfig.real_data_dir`

You can also override loading by providing a custom loader:

```python
from harness.data.graph_generator import GraphGeneratorConfig, GraphGenerator

def my_loader(config: GraphGeneratorConfig):
    # return nodes, edges
    return {}, {}

config = GraphGeneratorConfig(use_real_data=True, real_data_loader=my_loader)
graph = GraphGenerator(config=config)
```

## Schema Updates

`InMemoryGraphSchema` caches type-level labels. If you mutate nodes or edges
after creation, call `mark_dirty()` or `rebuild()` before querying schema data.

## Local Dev (Python 3.11 + uv)

In this repo, each Python plugin keeps its own virtual environment at `.venv/`
(gitignored).

One-time venv creation:

```bash
cd geaflow-ai/plugins/casts
[ -d .venv ] || python3.11 -m venv .venv
```

Sync dependencies:

```bash
cd geaflow-ai/plugins/casts
uv sync --extra dev
```

Notes:

- You don't need to `source .venv/bin/activate` for normal workflows.
  - `uv sync` installs into the project env (`.venv/`)
  - `uv run ...` executes inside that env
- If you *do* activate a venv for interactive work, `uv sync --active` forces syncing
  into the active environment.

## Running a Simulation

From the CASTS plugin directory:

```bash
cd geaflow-ai/plugins/casts
uv sync --extra harness
uv run python -m harness.simulation.runner
```

## Running the Service (FastAPI)

Start the CASTS decision service (used by GeaFlow Java integration):

```bash
cd geaflow-ai/plugins/casts
uv sync --extra dev
uv run uvicorn api.app:app --host 127.0.0.1 --port 5001
```

Java side configuration (defaults shown):

- `GEAFLOW_AI_CASTS_URL=http://localhost:5001`
- `GEAFLOW_AI_CASTS_TOKEN=` (optional bearer token)

One-click smoke test (starts/stops the server as needed):

```bash
cd geaflow-ai/plugins/casts
./scripts/smoke.sh
```

## Tests

Run tests locally:

```bash
cd geaflow-ai/plugins/casts
uv sync --extra dev
uv run pytest -q
```

## Lint & Type Check

Run lint (ruff) and type checks (mypy):

```bash
cd geaflow-ai/plugins/casts
uv sync --extra dev
uv run ruff format --check .
uv run ruff check .
uv run mypy -p api -p core -p services -p harness
```

CI: `.github/workflows/ci-py311.yml` runs the CASTS + LightMem Python tests on
Python 3.11 via `uv`.

## Documentation

- `docs/dependency_licenses.md`: Direct-dependency license summary and verification notes.

## Dependency Mirrors

This project does not hardcode PyPI mirrors. Configure mirrors in your
environment or tooling if needed.

## Third-Party Licenses

See `docs/dependency_licenses.md` for a direct-dependency license summary.
