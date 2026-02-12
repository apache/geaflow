# CASTS (Context-Aware Strategy Cache System)

CASTS is a strategy cache for graph traversal systems. It learns, stores, and
retrieves traversal decisions (strategies) based on structural signatures,
node properties, and goals, with optional LLM-driven guidance.

## Name Origin

CASTS stands for **Context-Aware Strategy Cache System**.

## Design Goals

- Cache traversal strategies (SKUs) to reduce repeated LLM calls.
- Separate schema metadata from execution logic.
- Support both synthetic and real-world graph data.
- Keep the core cache logic deterministic and testable.

## Module Layout

- `core`: cache, schema, configuration, and core models
- `data`: data sources and graph generation (synthetic + real)
- `services`: embedding + LLM services
- `simulation`: simulation engine and evaluation
- `tests`: unit and integration tests

## Repository Placement

This module is intended to live under `geaflow-ai/plugins/casts` as a standalone
plugin, with the Python package located at the module root.

## Configuration (Required)

The following environment variables are required. Missing values raise a
`ValueError` at startup:

- `EMBEDDING_ENDPOINT`
- `EMBEDDING_APIKEY`
- `EMBEDDING_MODEL`
- `LLM_ENDPOINT`
- `LLM_APIKEY`
- `LLM_MODEL`

You can use a local `.env` file for development. The code does not provide
automatic fallbacks for missing credentials.

## Real Data Loading

The default loader reads CSV files from `data/real_graph_data` (or
`real_graph_data`) and builds a directed graph. You can override this behavior
by providing a custom loader:

```python
from data.graph_generator import GraphGeneratorConfig, GraphGenerator

def my_loader(config: GraphGeneratorConfig):
    # return nodes, edges
    return {}, {}

config = GraphGeneratorConfig(use_real_data=True, real_data_loader=my_loader)
graph = GraphGenerator(config=config)
```

## Schema Updates

`InMemoryGraphSchema` caches type-level labels. If you mutate nodes or edges
after creation, call `mark_dirty()` or `rebuild()` before querying schema data.

## Running a Simulation

From the plugins directory (parent of this module):

```bash
cd /Users/kuda/code/geaflow/geaflow-ai/plugins
uv sync --frozen
python -m simulation.runner
```

## Tests

Run tests locally:

```bash
uv sync --frozen
pytest
```

There is no GitHub Actions workflow for this module by default.

## Documentation

- `docs/dependency_licenses.md`: Direct-dependency license summary and verification notes.

## Dependency Mirrors

This project does not hardcode PyPI mirrors. Configure mirrors in your
environment or tooling if needed.

## Third-Party Licenses

See `docs/dependency_licenses.md` for a direct-dependency license summary.
