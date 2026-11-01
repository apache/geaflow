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
- Gold records (`GEAFLOW_GOLD`), either a JSON/zip file or a directory holding the
  Hugging Face parquet. Without gold the run still exercises every method and
  reports latency/cost, but leaves Recall/MRR/NDCG empty rather than fabricating
  numbers.

## Usage

```bash
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_GOLD=tmp/graphrag/data/cache/2wikimultihopqa-dev.zip
# HotpotQA ships as parquet inside a cache directory:
# export GEAFLOW_GOLD=tmp/graphrag/data/cache/hotpotqa-hf/distractor-validation.parquet

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

## Measured full runs

Both fixed datasets were run end to end at full size. Every request returned 200.

### 2WikiMultiHopQA dev - 12,576 queries x 6 methods (75,456 requests)

Artifact: `tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261101T040443Z/`

| Method | Recall@10 | MRR | NDCG@10 | p50 ms | p95 ms | edges | tokens |
|---|---|---|---|---|---|---|---|
| BM25_ONLY | 0.0020 | 0.0037 | 0.0023 | 9.1 | 18.8 | 0 | 2723 |
| VECTOR_ONLY | 0.0020 | 0.0034 | 0.0022 | 4.4 | 14.2 | 0 | 2474 |
| VECTOR_OVERFETCH | 0.0020 | 0.0034 | 0.0022 | 8.6 | 16.9 | 0 | 10126 |
| GRAPH_ONLY | **0.7141** | **0.6344** | **0.6001** | 39.5 | 62.3 | 99.5 | 2066 |
| FIXED_HYBRID | 0.5605 | 0.2805 | 0.3328 | 42.5 | 72.3 | 78.7 | 2725 |
| ADAPTIVE_HYBRID | 0.6411 | 0.1534 | 0.2695 | 36.5 | 65.5 | 39.7 | 2669 |

### HotpotQA distractor validation - 7,405 queries x 6 methods (44,430 requests)

Artifact: `tmp/graphrag/data/runs/reproduce-hotpotqa-20261101T045619Z/`

| Method | Recall@10 | MRR | NDCG@10 | p50 ms | p95 ms | edges |
|---|---|---|---|---|---|---|
| BM25_ONLY | 0.0024 | 0.0047 | 0.0029 | 8.3 | 13.6 | 0 |
| VECTOR_ONLY | 0.0024 | 0.0046 | 0.0029 | 5.9 | 10.1 | 0 |
| VECTOR_OVERFETCH | 0.0024 | 0.0046 | 0.0029 | 10.2 | 16.2 | 0 |
| GRAPH_ONLY | **0.5350** | **0.3931** | **0.4030** | 31.6 | 44.5 | 74.2 |
| FIXED_HYBRID | 0.4358 | 0.0937 | 0.1805 | 36.9 | 50.7 | 81.2 |
| ADAPTIVE_HYBRID | 0.3818 | 0.0723 | 0.1480 | 29.6 | 46.7 | 35.1 |

## Negative result for Adaptive Hybrid

**This is a negative result and it is reported as such.** On both datasets
Graph-only leads every quality metric. Adaptive Hybrid is consistently the
cheapest graph-expanding method (35-40 edges vs 74-100) and beats Fixed Hybrid on
recall, but its MRR and NDCG are the lowest of the graph-bearing methods and on
HotpotQA it is also below Fixed Hybrid on recall.

BM25 and vector recall near zero means lexical and semantic similarity alone
almost never surface the gold supporting chunk; graph expansion is what supplies
the evidence. That confirms the design's premise that graph retrieval is
necessary, and refutes the premise that the current deterministic routing policy
improves ranking quality over simply running graph-only.

Applicability boundary: the routing policy buys cost, not precision. Until a
policy is shown to raise MRR/NDCG above graph-only under the same budget, it
should be treated as a cost-control mechanism rather than a quality lever.
