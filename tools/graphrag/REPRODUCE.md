# Reproducing the GraphRAG evaluation

`reproduce.sh` is the single entry point for the full pipeline. It imports and
publishes a READY artifact bundle, starts the retrieval service, waits for
`/ready`, runs the six-method benchmark, and records every artifact it produced
in one run directory.

## Prerequisites

- JDK and Maven available offline (the script builds with `-o`).
- A dataset manifest under `tools/graphrag/manifests/`.
- Precomputed query vectors (`GEAFLOW_VECTORS`, `.npz` with `ids` and `vectors`)
  — required because the vector channel and the equal-budget Vector Overfetch
  baseline cannot be reproduced without them.
- A query JSONL (`GEAFLOW_QUERIES`).
- Optional gold records (`GEAFLOW_GOLD`). Without gold the run still exercises
  every method and reports latency/cost, but leaves Recall/MRR/NDCG empty
  rather than fabricating numbers.

## Usage

```bash
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_GOLD=tmp/graphrag/data/cache/2wikimultihopqa-dev.zip

# full run: import -> publish -> serve -> benchmark
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa

# reuse an existing READY bundle (no re-import)
GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle \
  tools/graphrag/reproduce.sh --dataset 2wikimultihopqa --skip-build --limit 20
```

## Outputs

`reproduce-manifest.json` lists every file. The important ones:

| File | Contents |
|---|---|
| `<dataset>-six-methods.jsonl.gz` | Per-request record: status, evidence, trace, metrics (gzip; a full run reaches multi-GB uncompressed) |
| `<dataset>-six-methods.csv` | Per-method summary: Recall@K, MRR, NDCG, path hit, EM/F1, P50/P95/P99, candidate/edge/token cost, policy id, code revision, seed |
| `ready.json` | Service readiness captured at startup |
| `service.log` | Service stdout/stderr |

## Measured full run

The complete 2Wiki dev split (12,576 queries x 6 methods = 75,456 requests) was
run against the published bundle; every request returned 200. Artifact:
`tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261101T040443Z/`.

| Method | Recall@10 | MRR | NDCG@10 | p50 ms | p95 ms | edges | tokens |
|---|---|---|---|---|---|---|---|
| BM25_ONLY | 0.0020 | 0.0037 | 0.0023 | 9.1 | 18.8 | 0 | 2723 |
| VECTOR_ONLY | 0.0020 | 0.0034 | 0.0022 | 4.4 | 14.2 | 0 | 2474 |
| VECTOR_OVERFETCH | 0.0020 | 0.0034 | 0.0022 | 8.6 | 16.9 | 0 | 10126 |
| GRAPH_ONLY | 0.7141 | 0.6344 | 0.6001 | 39.5 | 62.3 | 99.5 | 2066 |
| FIXED_HYBRID | 0.5605 | 0.2805 | 0.3328 | 42.5 | 72.3 | 78.7 | 2725 |
| ADAPTIVE_HYBRID | 0.6411 | 0.1534 | 0.2695 | 36.5 | 65.5 | 39.7 | 2669 |

**This is a negative result for Adaptive Hybrid and it is reported as such.**
Graph-only leads on every quality metric. Adaptive Hybrid beats Fixed Hybrid on
recall (0.641 vs 0.561) and is cheaper in edge scans (39.7 vs 78.7), but its MRR
(0.153) and NDCG (0.270) are well below Graph-only and below Fixed Hybrid on MRR,
and it never produced a relation-path hit. The adaptive policy therefore buys
recall and cost, not ranking precision; it does not presently justify replacing
the fixed or graph-only configurations for answer quality. BM25 and vector recall
near zero means lexical/semantic similarity alone almost never surfaces the gold
supporting chunk on this dataset.

## Limits

A full-dataset run and the long production load profile need the complete local
cache and a host sized for the artifact; this repository only ships the smoke
evidence produced in-session. Record the `codeRevision` column with any result
you publish.
