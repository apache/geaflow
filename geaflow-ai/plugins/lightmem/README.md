# LightMem (Lightweight Context Memory)

LightMem is a ledger-backed memory kernel for AI agents. It stores, retrieves,
and traces context memories through an append-only event ledger and vector-indexed
views, with strict scope isolation and full provenance on every recall.

## Name Origin

**LightMem** stands for **Lightweight Context Memory** — a minimal but complete
memory system that stays lightweight enough for local dev and deterministic testing,
while following a protocol designed for production use.

## Design Goals

- **Ledger-centric**: An append-only event log is the single source of truth for
  all memory writes, updates, and deletions.
- **Scope-bounded isolation**: Every read and write is bound to a scope
  (tenant/user/agent/run/actor). No cross-scope memory leakage.
- **Provenance-first**: Every recalled memory carries an evidence chain back to
  the ledger event that created it (injection closure, evidence closure, decision
  closure).
- **Minimal viable kernel**: Ships with hash-based embeddings for deterministic
  testing; designed to be pluggable for production embedding backends.

## Two Modes: Production vs. Echo

### Production Mode (GeaFlow Java Integration)

In production, the **GeaFlow Java data plane** acts as a proxy. The Java
`GeaFlowMemoryServer` accepts `/memory/write` and `/memory/recall` requests,
extracts scope from query parameters, and forwards them to the Python LightMem
service via `LightMemRestClient`.

Key integration files:

- Java client: `geaflow-ai/src/main/java/org/apache/geaflow/ai/memory/LightMemRestClient.java`
- Java high-level API: `geaflow-ai/src/main/java/org/apache/geaflow/ai/memory/MemoryClient.java`
- Java server: `geaflow-ai/src/main/java/org/apache/geaflow/ai/GeaFlowMemoryServer.java`
- Python service: `geaflow-ai/plugins/lightmem/api/app.py`

### Echo Mode (Smoke Testing)

Setting `mode="echo"` in a write request extracts user messages directly as
memory units without invoking an LLM. This is the default mode, intended for
local development, CI, and integration smoke tests.

A full LLM-driven pipeline (`mode="llm"`) following the 5-step write protocol
(Scope → Unitize → Ground → Decide → Commit) is defined in `AGENTS.md` and
is a planned extension.

## Architecture

### Write Pipeline

```
POST /memory/write (Envelope)
  │
  ├─ Validate scope (reject if empty)
  ├─ Unitize: extract user messages → Action(ADD) list
  ├─ Commit: for each action:
  │    ├─ ledger.append(scope, action) → LedgerEvent
  │    └─ views.vector_view.upsert(unit)
  └─ Return actions + provenance (event_ids)
```

### Recall Pipeline

```
POST /memory/recall (Envelope)
  │
  ├─ Validate scope (reject if empty)
  ├─ views.vector_view.recall(query, scope_filter, limit)
  │    ├─ Embed query
  │    ├─ Cosine-similarity search
  │    ├─ Filter by scope (strict match)
  │    └─ Return top-k hits
  └─ Return units + provenance (memory_id, view_source, last_event_id, similarity)
```

## Module Layout

- `core/`: Memory kernel — `MemoryKernel`, `Ledger`, `VectorView`, core data types
- `api/`: FastAPI HTTP service — endpoints, envelope schema, error handling
- `tests/`: Unit and integration tests
- `scripts/`: `smoke.sh` one-click integration test

## API Endpoints

All requests (except `/health`) use a standard JSON envelope:

```json
{
  "api_version": "v1",
  "scope": { "user_id": "u1", "agent_id": "a1" },
  "trace": { "trace_id": "tr_...", "timestamp": 1700000000, "caller": "my-app" },
  "payload": { ... }
}
```

| Endpoint | Method | Payload | Description |
|---|---|---|---|
| `/health` | GET | — | Returns `{"status": "UP"}` |
| `/memory/write` | POST | `messages`, `mode` | Write memories from messages |
| `/memory/recall` | POST | `query`, `limit` | Recall memories by similarity |

Scope must contain at least one non-empty field (`tenant_id`, `user_id`,
`agent_id`, `run_id`, or `actor_id`). Requests with an empty scope are
rejected with HTTP 422.

## Configuration

Java-side environment variables (for the GeaFlow proxy):

- `GEAFLOW_AI_LIGHTMEM_URL` — LightMem service URL (default: `http://localhost:5002`)
- `GEAFLOW_AI_LIGHTMEM_TOKEN` — Optional bearer token for authentication

Smoke-test environment variables:

- `GEAFLOW_AI_LIGHTMEM_HOST` — Host to bind (default: `127.0.0.1`)
- `GEAFLOW_AI_LIGHTMEM_PORT` — Port to bind (default: `5002`)
- `GEAFLOW_AI_LIGHTMEM_TIMEOUT_SECONDS` — Server startup timeout (default: `20`)

## Local Dev (Python 3.11 + uv)

Each Python plugin keeps its own virtual environment at `.venv/` (gitignored).

One-time venv creation:

```bash
cd geaflow-ai/plugins/lightmem
[ -d .venv ] || python3.11 -m venv .venv
```

Sync dependencies:

```bash
cd geaflow-ai/plugins/lightmem
uv sync --extra dev
```

Notes:

- You don't need to `source .venv/bin/activate` for normal workflows.
  - `uv sync` installs into the project env (`.venv/`)
  - `uv run ...` executes inside that env
- If you *do* activate a venv for interactive work, `uv sync --active` forces syncing
  into the active environment.

## Running the Service (FastAPI)

Start the LightMem service:

```bash
cd geaflow-ai/plugins/lightmem
uv sync --extra dev
uv run uvicorn api.app:app --host 127.0.0.1 --port 5002
```

One-click smoke test (starts/stops the server as needed):

```bash
cd geaflow-ai/plugins/lightmem
./scripts/smoke.sh
```

## Tests

Run tests locally:

```bash
cd geaflow-ai/plugins/lightmem
uv sync --extra dev
uv run pytest -q tests
```

## Lint & Type Check

Run lint (ruff) and type checks (mypy):

```bash
cd geaflow-ai/plugins/lightmem
uv sync --extra dev
uv run ruff format --check .
uv run ruff check .
# LightMem uses package-dir mapping; mypy needs a non-editable install.
uv sync --extra dev --no-editable
uv run --no-editable mypy -p lightmem -p api
```

CI: `.github/workflows/ci-py311.yml` runs LightMem + CASTS Python tests on
Python 3.11 via `uv`.
