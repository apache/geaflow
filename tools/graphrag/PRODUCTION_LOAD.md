# GraphRAG production load run

The runner exercises the local HTTP retrieval service against a complete query/gold JSONL corpus. It uses fixed open-loop arrivals, does not retry requests, and records every
request and response. The output is a local capacity baseline, not a deployment SLO.

## Inputs

From the repository root, use these verified artifacts:

```bash
export GRAPHRAG_BUNDLE="$PWD/tmp/graphrag/data/runs/2wikimultihopqa-bundle"
export GRAPHRAG_RUNS="$PWD/tmp/graphrag/data/runs"
export GRAPHRAG_QUERIES="$PWD/tmp/graphrag/local_adapters/<corpus>/queries.jsonl"
export GRAPHRAG_VECTORS="$GRAPHRAG_RUNS/2wikimultihopqa-query-vectors-gte.npz"
export GRAPHRAG_VERSION=e9b966b931bee494
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Start the service with the bundle registered at startup:

```bash
export GEAFLOW_SERVER_PORT=19005
export GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT="$GRAPHRAG_BUNDLE"
export GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME=2wikimultihopqa
export GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION="$GRAPHRAG_VERSION"
export GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION="$GRAPHRAG_VERSION"
export GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE=iic/nlp_gte_sentence-embedding_english-small
export GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION=v1.0.0
mvn -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true package
mvn -pl geaflow-ai dependency:build-classpath -Dmdep.outputFile=/tmp/geaflow-ai-cp.txt
java -cp "geaflow-ai/target/classes:$(cat /tmp/geaflow-ai-cp.txt)" \
  org.apache.geaflow.ai.GeaFlowMemoryServer
```

Keep the service running in a separate terminal. Confirm `http://127.0.0.1:19005/ready` returns
`READY` with the requested graph version before starting a run.

## Smoke run

This runs all six methods and targeted cases with a small request count, then runs the three
failure drills. It validates response shape, budget trace, service metrics, resource sampling,
and output generation before the long profile.

```bash
python -m tools.graphrag.production_load \
  --url http://127.0.0.1:19005 \
  --queries "$GRAPHRAG_QUERIES" --vectors "$GRAPHRAG_VECTORS" \
  --bundle-root "$GRAPHRAG_BUNDLE" --graph-name 2wikimultihopqa \
  --graph-version "$GRAPHRAG_VERSION" --index-version "$GRAPHRAG_VERSION" \
  --server-pid "$GRAPHRAG_SERVER_PID" --output-dir "$GRAPHRAG_RUNS/production-load" \
  --run-id smoke --qps 1 --warmup-qps 1 --warmup-seconds 1 \
  --stage-seconds 1 --soak-seconds 0 --targeted-qps 1 --targeted-seconds 1 \
  --metrics-interval 1 --sample-interval 1
```

Review `acceptance.json`, `report.md`, `failure-drills.json`, and
`request-results.jsonl.gz`. Start the full profile only after the smoke acceptance passes.

## Full profile

The default profile performs a 30-second 5 QPS warmup per retrieval method, then 5, 20, and 50
QPS stages of two minutes each per method, followed by a 10-minute 50 QPS adaptive-hybrid soak.
It also runs focused 20 QPS scenarios for parallel retrieval, latency/token constraints, no-anchor,
high graph work, long queries, and multi-hop queries.

```bash
python -m tools.graphrag.production_load \
  --url http://127.0.0.1:19005 \
  --queries "$GRAPHRAG_QUERIES" --vectors "$GRAPHRAG_VECTORS" \
  --bundle-root "$GRAPHRAG_BUNDLE" --gold "$PWD/tmp/graphrag/local_adapters/<corpus>/gold.jsonl" \
  --graph-name 2wikimultihopqa --graph-version "$GRAPHRAG_VERSION" \
  --index-version "$GRAPHRAG_VERSION" --server-pid "$GRAPHRAG_SERVER_PID" \
  --output-dir "$GRAPHRAG_RUNS/production-load"
```

The run writes a timestamped directory containing the manifest, request traces, phase summary,
resource samples, service metrics, quality diagnostics, failure drills, and acceptance result.
Typed `RETRIEVAL_TIMEOUT` responses are counted separately and do not count as protocol errors;
unexpected server errors, transport failures, client drops, protocol violations, and hard budget
violations fail acceptance.

## Latency SLO

A run without thresholds only shows that requests succeeded, not that latency was acceptable, so
`acceptance.json` reports `latencySloConfigured: false` and the run cannot be claimed to meet an
SLO. Set explicit ceilings to make latency part of acceptance:

```bash
  --slo-p95-ms 200 --slo-p99-ms 500
```

Each phase is then compared against both ceilings, every breach is appended to `failures` as
`METHOD/phase: p95 ...`, and `acceptance.json` records `p95LimitMs`/`p99LimitMs` with
`latencySloConfigured: true`. Published reference values for the 2Wiki dev bundle are
**P95 <= 200 ms and P99 <= 500 ms at 50 QPS**; treat them as a starting point to re-baseline on your
own hardware, and always report the `codeRevision` alongside the result.

The 2026-10-18 full profile (`production-load/20261018T132744Z`) was recorded before any SLO
existed and its `acceptance.json` is `passed: false` with `latencySloConfigured: false`. It stays in
the repository as a historical capacity baseline and is not evidence of SLO compliance; re-run the
profile with the flags above before quoting latency guarantees.
