# GraphRAG Operations Guide

The retrieval service is exposed by `GeaFlowMemoryServer` (Solon) on the
`geaflow-ai` module. This guide is the release-facing contract for the
retrieval API, its configuration, its failure modes and its operational
diagnostics.

## 1. Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/retrievals` | Versioned structured retrieval (evidence, paths, sources, trace, cost) |
| `POST` | `/api/v1/answers` | Extractive answer/context over retrieved evidence, with citations and abstention |
| `GET` | `/health` | Process liveness only; says nothing about artifacts |
| `GET` | `/ready` | Artifact/manifest/lease readiness; `200` when READY, `503` otherwise |
| `GET` | `/metrics/retrieval` | Machine-readable counters, latency percentiles and cost totals |
| `POST` | `/query/context` | Legacy keyword session: create context |
| `POST` | `/query/exec` | Legacy keyword session: execute query |
| `POST` | `/query/result` | Legacy keyword session: fetch result |
| `POST` | `/graph/create`, `/graph/addEntitySchema`, `/graph/getGraphSchema`, `/graph/insertEntity`, `/graph/delEntity` | Legacy graph mutation API |

`X-Request-Id` is echoed on every response. An unsafe or over-long value is
replaced by a generated UUID rather than rejected.

### Answer endpoint

`POST /api/v1/answers` runs a retrieval for `question` and returns a constrained
extractive answer. The response carries `answer`, `abstentionReason`,
`citations` (each pointing at a returned evidence id plus source/ chunk/path),
`context`, `tokens`, `citationCoverage`, `suppressReasons` (why each offered
evidence item was withheld: `DUPLICATE_CHUNK`, `TOKEN_BUDGET_EXCEEDED`,
`INVALID_PROVENANCE`) and the echoed `requestId`/`graphVersion`/`indexVersion`.

Request fields: `question` (required), `tokenBudget` (required), and optional
`requestId`, `graphName`, `graphVersion`, `indexVersion`, `mode`. Absent
versions fall back to the configured artifact identity and an absent mode falls
back to `retrieval.default-mode`. An unsupported request returns `200` with a
structured `abstentionReason` rather than invented text; a malformed body
returns a typed `400`.

```bash
curl -H 'Content-Type: application/json' -H 'X-Request-Id: answer-1' \
  -d '{"question":"Who taught ethics?","tokenBudget":64,"mode":"KEYWORD",
       "graphName":"Confucius","graphVersion":"v4","indexVersion":"v4"}' \
  http://127.0.0.1:8080/api/v1/answers
```

## 2. Retrieval modes

Five modes are accepted by `mode`. All five are reachable over HTTP.

| Mode | Channels | Notes |
|---|---|---|
| `KEYWORD` | legacy keyword index | Alias of `BM25_ONLY`; retained for backward compatibility |
| `BM25_ONLY` | BM25 | Lucene-backed lexical retrieval |
| `VECTOR_ONLY` | vector | Requires a precomputed `queryVector` |
| `GRAPH_ONLY` | graph | Requires entity anchors; returns structured empty results when none are found |
| `HYBRID` | BM25 + vector + graph | Fused, reranked and truncated |

## 3. Execution modes

Three execution modes are accepted by `executionMode`.

| Mode | Behaviour |
|---|---|
| `SEQUENTIAL` | Channels run in order; completed results can guide later stages |
| `PARALLEL` | Channels run concurrently inside the shared stage deadline; readers, leases, traces and exceptions are isolated |
| `CASCADED` | A cheap BM25/vector probe produces anchors and online statistics, then decides whether the graph stage runs |

## 4. Request fields

Required: `graphName`, `query`.

Optional: `requestId`, `mode`, `executionMode`, `queryVector`,
`vectorSource`, `vectorVersion`, `graphVersion`, `indexVersion`,
`exactRanking`, `trace` and `budget`.

`budget` accepts `topK`, `timeoutMs`, `maxCandidates`, `tokenBudget` and
`primaryConstraint`. Missing values fall back to the versioned server
defaults in section 6.

## 5. Response shape

The response carries `requestId`, `graphName`, `graphVersion`, `evidence`,
`paths`, `sources`, `degradedChannels`, `effectiveBudget` and `trace`.

The trace records, among other fields: selected channels, channel budgets,
per-stage status and elapsed time, stop reasons, degradation reasons,
`policyId`, `policyVersion`, `policyParameters`, `featureSchemaVersion`,
`routingFeatures`, `routingReasons`, `replanEvents`, `tokenEstimatorVersion`,
`rerankerVersion`, `fusionVersion`, `edgesExamined`, `tokensRetained` and
`evaluatedCounts`.

## 6. Configuration defaults and hard ceilings

All values below come from `geaflow-ai/src/main/resources/application.yml`
and can be overridden with the listed environment variable.

| Property | Env var | Default | Hard ceiling |
|---|---|---|---|
| `retrieval.config-version` | — | `v1` | — |
| `retrieval.ready-graph-name` | — | `Confucius` | — |
| `retrieval.default-mode` | — | `KEYWORD` | — |
| `retrieval.default-execution-mode` | — | `SEQUENTIAL` | — |
| `retrieval.default-top-k` | — | `10` | `max-top-k` |
| `retrieval.max-top-k` | — | `100` | — |
| `retrieval.default-timeout-ms` | — | `3000` | `max-timeout-ms` |
| `retrieval.max-timeout-ms` | — | `10000` | — |
| `retrieval.default-max-candidates` | — | `100` | `max-candidates` |
| `retrieval.max-candidates` | — | `1000` | — |
| `retrieval.default-token-budget` | — | `4096` | `max-token-budget` |
| `retrieval.max-token-budget` | — | `16384` | — |
| `retrieval.remote-access-enabled` | `GEAFLOW_REMOTE_ACCESS_ENABLED` | `false` | — |
| `retrieval.api-token` | `GEAFLOW_RETRIEVAL_API_TOKEN` | empty | — |
| `retrieval.artifact-bundle-root` | `GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT` | empty | — |
| `retrieval.artifact-graph-name` | `GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME` | empty | — |
| `retrieval.artifact-graph-version` | `GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION` | empty | — |
| `retrieval.artifact-index-version` | `GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION` | empty | — |
| `retrieval.artifact-graph-path` | `GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_PATH` | `graph.bin` | — |
| `retrieval.artifact-bm25-path` | `GEAFLOW_RETRIEVAL_ARTIFACT_BM25_PATH` | `bm25` | — |
| `retrieval.artifact-vector-path` | `GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_PATH` | `vector.bin` | — |
| `retrieval.artifact-vector-source` | `GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE` | empty | — |
| `retrieval.artifact-vector-version` | `GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION` | empty | — |
| `retrieval.routing-policy-id` | `GEAFLOW_RETRIEVAL_ROUTING_POLICY_ID` | `deterministic-rule` | allow-list: `deterministic-rule` |
| `retrieval.max-replan-count` | `GEAFLOW_RETRIEVAL_MAX_REPLAN_COUNT` | `1` | `[0, 4]` |
| `retrieval.strict-plan-validation` | `GEAFLOW_RETRIEVAL_STRICT_PLAN_VALIDATION` | `true` | — |
| `retrieval.reranker-id` | `GEAFLOW_RETRIEVAL_RERANKER_ID` | `deterministic-reranker-v1` | allow-list |
| `retrieval.token-estimator-id` | `GEAFLOW_RETRIEVAL_TOKEN_ESTIMATOR_ID` | `chars4-v1` | allow-list |
| `retrieval.deterministic-rule.high-average-idf` | `GEAFLOW_RETRIEVAL_HIGH_AVERAGE_IDF` | `1.5` | finite, `>= 0` |
| `retrieval.deterministic-rule.high-score-gap` | `GEAFLOW_RETRIEVAL_HIGH_SCORE_GAP` | `0.20` | finite, `>= 0` |
| `retrieval.deterministic-rule.long-query-length` | `GEAFLOW_RETRIEVAL_LONG_QUERY_LENGTH` | `12` | `>= 1` |
| `retrieval.deterministic-rule.strong-anchor-confidence` | `GEAFLOW_RETRIEVAL_STRONG_ANCHOR_CONFIDENCE` | `0.75` | `[0, 1]` |
| `retrieval.deterministic-rule.minimum-anchor-count` | `GEAFLOW_RETRIEVAL_MINIMUM_ANCHOR_COUNT` | `1` | `>= 1` |
| `retrieval.deterministic-rule.high-candidate-degree` | `GEAFLOW_RETRIEVAL_HIGH_CANDIDATE_DEGREE` | `32` | `>= 1` |
| `retrieval.deterministic-rule.high-growth-rate` | `GEAFLOW_RETRIEVAL_HIGH_GROWTH_RATE` | `2.0` | finite, `>= 0` |
| `retrieval.deterministic-rule.base-neighbor-cap` | `GEAFLOW_RETRIEVAL_BASE_NEIGHBOR_CAP` | `16` | `>= 1` |

An out-of-range or unknown value fails `/ready` with
`INVALID_CONFIGURATION`; it never silently falls back to a default.

## 7. Error codes

Every failure is a typed `RetrievalError` with `requestId`, `code`,
`message` and `retriable`.

| Code | HTTP | Retriable | Meaning |
|---|---|---|---|
| `INVALID_REQUEST` | 400 | no | Malformed JSON, missing required field, or a value that fails validation |
| `UNSUPPORTED_OPTION` | 400 | no | A mode/channel combination the caller requested is not supported |
| `AUTHENTICATION_REQUIRED` | 401 | no | Remote access is enabled and no Bearer token was supplied |
| `FORBIDDEN` | 403 | no | Bearer token missing, malformed or not matching `api-token` |
| `GRAPH_NOT_FOUND` | 404 | no | The requested graph is not loaded |
| `INDEX_NOT_READY` | 503 | yes | Required artifact is missing, corrupt, or not in READY state |
| `RETRIEVAL_TIMEOUT` | 504 | yes | The shared request deadline was exceeded |
| `INTERNAL_ERROR` | 500 | no | Unexpected server fault; the message is intentionally generic |

## 8. Examples

### 8.1 Successful hybrid retrieval

```bash
curl -sS -H 'Content-Type: application/json' -H 'X-Request-Id: demo-1' \
  -d '{"graphName":"Confucius","query":"teacher","mode":"HYBRID",
       "executionMode":"SEQUENTIAL","queryVector":[0.3,0.4],
       "graphVersion":"v1","indexVersion":"v1",
       "budget":{"topK":5,"timeoutMs":3000,"maxCandidates":30,"tokenBudget":4096}}' \
  http://127.0.0.1:8080/api/v1/retrievals
```

### 8.2 Graph-only request with no reliable anchor

No anchor means a structured empty result, not an error: the response is
`200` with empty `evidence`, empty `paths`, and
`trace.channelStopReasons.GRAPH = "NO_RELIABLE_ANCHOR"`.

```bash
curl -sS -H 'Content-Type: application/json' \
  -d '{"graphName":"Confucius","query":"zzyzx nonexistent entity","mode":"GRAPH_ONLY",
       "budget":{"topK":5,"timeoutMs":3000,"maxCandidates":30,"tokenBudget":4096}}' \
  http://127.0.0.1:8080/api/v1/retrievals
```

### 8.3 Partial result under a token budget

When the token budget is exhausted mid-stage, the request still returns
`200` with the evidence gathered so far, `trace.stopReason` of
`TOKEN_BUDGET`, and the retained evidence counted in `trace.tokensRetained`.

```bash
curl -sS -H 'Content-Type: application/json' \
  -d '{"graphName":"Confucius","query":"teacher","mode":"HYBRID",
       "queryVector":[0.3,0.4],"graphVersion":"v1","indexVersion":"v1",
       "budget":{"topK":50,"timeoutMs":3000,"maxCandidates":200,"tokenBudget":32,
                 "primaryConstraint":"TOKENS"}}' \
  http://127.0.0.1:8080/api/v1/retrievals
```

### 8.4 Typed error

```bash
curl -sS -o /dev/null -w '%{http_code}\n' -H 'Content-Type: application/json' \
  -d '{"graphName":"missing-graph","query":"teacher"}' \
  http://127.0.0.1:8080/api/v1/retrievals
# 404
# {"requestId":"...","code":"GRAPH_NOT_FOUND","message":"graph not found: missing-graph",
#  "retriable":false}
```

### 8.5 Java client

```java
RetrievalRequest request = new RetrievalRequest();
request.setGraphName("Confucius");
request.setQuery("teacher");
request.setMode("HYBRID");
request.setQueryVector(Arrays.asList(0.3, 0.4));
request.setBudget(new RetrievalBudget(5, 3000, 30, 4096));
String body = RetrievalApiJson.toJson(request);
// POST body to /api/v1/retrievals, then:
RetrievalResponse response = RetrievalApiJson.parseResponse(httpResponseBody);
```

## 9. Datasets and licences

Benchmarks use two public multi-hop QA datasets. Raw data is never committed
to the repository; it is fetched or read from a pinned local cache and
verified against the manifest checksum before use.

| Dataset | Split | Manifest | Licence |
|---|---|---|---|
| HotpotQA (official distractor) | validation/dev | `tools/graphrag/manifests/hotpotqa-distractor-v2.json` | CC BY-SA 4.0 |
| 2WikiMultiHopQA (official) | dev | `tools/graphrag/manifests/2wikimultihopqa-dev-v2.json` | Apache-2.0 |

Each manifest pins the dataset release, parser version, preprocessing
version, chunking configuration, vector source and random seed so an
artifact digest can be reproduced.

## 10. Cache requirements

`${GRAPH_RAG_CACHE}` must contain the dataset archives named by each
manifest `cache_path`. The loader rejects a source whose SHA-256 does not
match the manifest, whose size exceeds `max_download_bytes`, or whose zip
contains more than `max_zip_entries` entries. Parquet input is accepted only
for HotpotQA; 2WikiMultiHopQA is read from its archive JSON.

## 11. Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `/ready` returns `503` with `GRAPH_NOT_LOADED` | No graph loaded for the configured name | Build and publish the artifact, or set `artifact-bundle-root` |
| `/ready` returns `503` with `KEYWORD_INDEX_NOT_READY` | Keyword index not initialized | Complete the `INDEXING` state transition |
| `/ready` returns `503` with `INVALID_CONFIGURATION` | A property failed validation | Check section 6 ranges and allow-lists |
| `401` / `403` on `/health` or `/ready` | `remote-access-enabled` is on | Send `Authorization: Bearer <api-token>` |
| `INDEX_NOT_READY` on `VECTOR_ONLY` | No vector artifact, or `vectorSource`/`vectorVersion` mismatch | Publish a matching vector artifact |
| `UNSUPPORTED_OPTION` on `VECTOR_ONLY` | `queryVector` missing | Supply a precomputed vector |
| `GRAPH_ONLY` returns empty evidence | No entity anchor above the confidence floor | Expected; see section 8.2 |
| `trace.channelStopReasons.GRAPH = "LOW_ASSOCIATION_SCORE"` | Too few edges passed the association floor and the hop was abandoned | Raise `minAssociationScore` precision or relax the survival rate |
| Replan events absent | `max-replan-count` is `0`, or the boundary decision was unchanged | Set `max-replan-count` above `0` |

## 12. Version matrix

| Component | Version recorded in trace |
|---|---|
| Retrieval contract | `trace.traceVersion` = `v1` |
| Configuration | `retrieval.config-version` = `v1` |
| Routing features | `trace.featureSchemaVersion` (`RoutingFeatures.SCHEMA_VERSION`) |
| Routing policy | `trace.policyId` + `trace.policyVersion` |
| Token estimator | `trace.tokenEstimatorVersion` |
| Reranker | `trace.rerankerVersion` |
| Fusion | `trace.fusionVersion` |
| Graph / index | `graphVersion` / `indexVersion` on the response |

## 13. Verification before release

```bash
mvn -o -pl geaflow-ai -DskipTests validate
mvn -o -pl geaflow-ai test -Djacoco.skip=true
python3 -m pytest tools/graphrag/tests -q
```

See `graphrag-release-checklist.md` for the full clean-checkout procedure.