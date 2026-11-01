# GraphRAG release checklist

Run these commands from a clean checkout with the repository's supported JDK
and a validated local artifact cache:

```bash
mvn -pl geaflow-ai -DskipTests validate
mvn -pl geaflow-ai test -Djacoco.skip=true -Drat.skip=true
mvn -pl geaflow-ai -Dtest=RetrievalServiceTest,RetrievalApiJsonTest,GraphRagMvpEndToEndTest test \
  -Djacoco.skip=true -Drat.skip=true
```

The import and artifact release path must validate a fixed manifest, write all
graph, BM25, and vector files under a staging directory, and publish one
`READY.json` atomically. Reuse is allowed only when checksums and manifest
identity match. Keep the manifest, `READY.json`, checksums, and benchmark
JSONL/CSV output with the release artifacts; do not commit raw datasets or
credentials.

The service contract is `POST /api/v1/retrievals`. Supported retrieval modes
are `BM25_ONLY`, `VECTOR_ONLY`, `GRAPH_ONLY`, and `HYBRID`; execution modes are
`SEQUENTIAL`, `PARALLEL`, and `CASCADED`. Requests may include a request ID,
version pins, top-k, timeout, candidate, and token budgets. Responses retain
evidence IDs, source and chunk provenance, budget decisions, trace events, and
typed errors. `/health` reports process liveness, `/ready` reports readiness
for the configured graph name, and `/metrics/retrieval` reports counters and latency
percentiles. When remote access is enabled, all four endpoints require the
configured Bearer token and echo `X-Request-Id`.

Exercise the success and failure paths after startup:

```bash
curl -H 'Content-Type: application/json' -H 'X-Request-Id: release-1' \
  -d '{"graphName":"Confucius","query":"teacher","mode":"HYBRID",'\
      '"executionMode":"SEQUENTIAL","budget":{"topK":5,"timeoutMs":3000}}' \
  http://127.0.0.1:8080/api/v1/retrievals
curl -H 'Content-Type: application/json' -H 'X-Request-Id: answer-1' \
  -d '{"question":"Who taught ethics?","tokenBudget":64,"mode":"KEYWORD",
       "graphName":"Confucius","graphVersion":"v4","indexVersion":"v4"}' \
  http://127.0.0.1:8080/api/v1/answers
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/ready
curl http://127.0.0.1:8080/metrics/retrieval
```

Record the exact dataset release, graph/index versions, configuration, JDK,
commit, and benchmark seed. Dataset adapters (`tools/graphrag/eval/retrieval.py`),
the six-method batch runner (`tools/graphrag/benchmark_http.py`) and cost
accounting (`meanTokensRetained`, `meanEdgesExamined`, independent edge-scan
budget) are implemented, and every benchmark row records `codeRevision`,
`datasetRelease`, `randomSeed`, `configVersion`, `policyId`,
`tokenEstimatorVersion` and `replanCount` so runs cannot be silently mixed.

Reproduce the pipeline end to end with one command:

```bash
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_GOLD=tmp/graphrag/data/cache/2wikimultihopqa-dev.zip
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa
```

See `tools/graphrag/REPRODUCE.md` for inputs and outputs. The generated
`<dataset>-six-methods.csv` carries `evidenceRecallAtK`, `mrr`, `ndcgAtK`,
`relationPathHit`, `answerExactMatch`, `answerF1` and P50/P95/P99 latency.

Release blockers include missing READY artifacts, non-deterministic ordering,
leaked readers or leases, untyped failures, and any command above failing in a
clean environment. The dataset licences are recorded as `pending-review`; obtain
approval before publishing artifacts derived from them.
