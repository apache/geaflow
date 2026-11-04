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
commit, and benchmark seed. Cost accounting (`meanTokensRetained`,
`meanEdgesExamined`, independent edge-scan budget) is implemented in this module,
and every benchmark row records `codeRevision`, `datasetRelease`, `randomSeed`,
`configVersion`, `policyId`, `tokenEstimatorVersion` and `replanCount` so runs
cannot be silently mixed.

> **Scope of this repository.** Only the retrieval engine and its unit tests are
> shipped here. Dataset ingestion drivers, the benchmark runner, load testing and
> report generation are operated locally and are not tracked in this repository,
> so the commands below reference them by path on the operator's machine.

This module exposes two local execution entry points, which a local pipeline can
drive by main class name:

| Entry point | Purpose |
| --- | --- |
| `org.apache.geaflow.ai.retrieval.ingest.CanonicalJsonlArtifactBridgeCli` | Publish a verified dataset release as graph / BM25 / vector artifacts plus `READY.json` |
| `org.apache.geaflow.ai.retrieval.service.RetrievalBatchCli` | Drive retrieval over a JSONL query file and write JSONL results |

`RetrievalBatchCli` reuses the same recall service as the server, so batch output
and online responses cannot diverge. Build and run it with:

```bash
mvn -pl geaflow-ai -o package -DskipTests -Drat.skip=true
java -cp <classpath> org.apache.geaflow.ai.retrieval.service.RetrievalBatchCli \
  --bundle <ready-bundle-dir> --queries <queries.jsonl> --output <results.jsonl> \
  --graph-name <name> --graph-version <v> --index-version <v> \
  --vector-source <src> --vector-version <v>
```

A six-method benchmark row is expected to carry `evidenceRecallAtK`, `mrr`,
`ndcgAtK`, `relationPathHit`, `answerExactMatch`, `answerF1`,
`primaryConstraint` and P50/P95/P99 latency.

Two of those columns depend on inputs that must be present, and a column is only
meaningful when it is populated:

- `answerExactMatch` and `answerF1` are computed from the extractive answer the
  retrieval response now carries (`RetrievalResponse.answer`). They are populated
  for every successful request. A column of empty values means the run used a
  binary that predates the extractive answer field, not that the answers were
  wrong.
- `relationPathHit` requires relation targets. 2WikiMultiHopQA provides explicit
  `evidences` triples; HotpotQA is not relation-annotated, so its target is
  derived from the supporting-fact titles. A single-paragraph answer has no
  inter-entity hop and therefore no relation path to hit; those queries score 0
  by definition rather than being counted as failures.

`primaryConstraint` records the ONE hard budget constraint the run spent, so an
equal-budget comparison cannot silently mix constraint kinds. Do not compare rows
whose `primaryConstraint` differs.

Release blockers include missing READY artifacts, non-deterministic ordering,
leaked readers or leases, untyped failures, and any command above failing in a
clean environment. The dataset licences are recorded as `pending-review`; obtain
approval before publishing artifacts derived from them.
