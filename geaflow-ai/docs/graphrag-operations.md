# GraphRAG Operations Guide

The retrieval endpoint is `POST /api/v1/retrievals`. It accepts `BM25_ONLY`,
`VECTOR_ONLY`, `GRAPH_ONLY`, and `HYBRID` modes, with `SEQUENTIAL`, `PARALLEL`,
or `CASCADED` execution. Requests may pin `graphVersion` and `indexVersion`,
provide a `queryVector`, and set `budget.topK`, `timeoutMs`,
`maxCandidates`, and `tokenBudget`. `X-Request-Id` is echoed in the response
and trace.

Use `/health` for process liveness, `/ready` for artifact readiness, and
`/metrics/retrieval` for machine-readable request counters and latency
percentiles. When remote access is enabled, these endpoints require the same
Bearer token as retrieval.

```bash
curl -H 'Content-Type: application/json' -H 'X-Request-Id: demo-1' \
  -d '{"graphName":"Confucius","query":"teacher",'\
      '"mode":"HYBRID","executionMode":"SEQUENTIAL",'\
      '"queryVector":[0.3,0.4],"graphVersion":"v1",'\
      '"indexVersion":"v1","budget":{"topK":5,"timeoutMs":3000,'\
      '"maxCandidates":30,"tokenBudget":4096}}' \
  http://127.0.0.1:8080/api/v1/retrievals
```

`INDEX_NOT_READY`, `RETRIEVAL_TIMEOUT`, and `INTERNAL_ERROR` are typed errors;
the response includes the request ID. A trace records selected channels,
budgets, stage status, degradation reason, policy metadata, and replan events.

The benchmark helpers in `org.apache.geaflow.ai.retrieval.evaluation` are
offline and deterministic. They do not download datasets or call a model
service. Dataset adapters must read only validated local manifest/cache files;
raw datasets and generated answers are not part of the release artifact.

Before release, run:

```bash
mvn -pl geaflow-ai -DskipTests validate
mvn -pl geaflow-ai test -Djacoco.skip=true -Drat.skip=true
```
