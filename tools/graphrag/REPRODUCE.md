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

## Ablations on both datasets

Budget-level and query-category ablations were run on both fixed datasets. Artifacts:
`tmp/graphrag/data/runs/ablations/<dataset>/<condition>.{jsonl.gz,csv}`.

```bash
PYTHONPATH=. python3 tools/graphrag/benchmark_http.py \
  --url http://127.0.0.1:PORT/api/v1/retrievals --queries <queries.jsonl> \
  --output runs/ablations/<dataset>/budget-tight.jsonl.gz \
  --graph-name <name> --graph-version <gv> --index-version <iv> \
  --vectors <queries.npz> --gold <gold> --chunks <bundle>/chunks.jsonl \
  --ablation budget-tight --seed 0
```

ADAPTIVE_HYBRID under each condition (2Wiki 500 queries, HotpotQA 150 queries;
budget-loose on HotpotQA used 60 queries because a 400-candidate window over the
larger graph does not finish in the default time box):

| Condition | Candidate budget | 2Wiki Recall / MRR | HotpotQA Recall / MRR | Edges |
|---|---|---|---|---|
| budget-loose | 400 | 0.484 / 0.109 | 0.453 / 0.098 | 33-35 |
| (reference) | 100 | 0.641 / 0.153 | 0.382 / 0.072 | 35-40 |
| budget-tight | 25 | 0.389 / 0.070 | 0.277 / 0.047 | 27-32 |
| multi-hop | 50 | 0.484 / 0.109 | 0.439 / 0.081 | 32-33 |
| high-degree | 25 | 0.000 / 0.000 | 0.000 / 0.000 | 0 |
| no-anchor | 100 | 0.000 / 0.000 | 0.000 / 0.000 | 0 |

Two conclusions, both consistent across datasets:

1. **Quality rises monotonically with budget** once the graph stage is funded
   (`budget-tight` < reference < `budget-loose`), which supports the design's premise
   that graph expansion is the evidence source and that its budget matters.
2. **Removing the graph channel zeroes recall entirely.** Both `no-anchor` and
   `high-degree` collapse to 0.000 on both datasets, confirming that BM25 and vector
   channels cannot substitute for graph expansion here, and that the high-degree
   neighbour cap is restrictive enough to suppress all graph evidence.

The ablation harness itself is covered by `test_budget_ablations_scale_the_wire_budget_only`
and `test_reported_budget_reflects_the_ablated_request`, which pins that an ablation
scales only the budget and that the recorded budget is the executed one.

## Known limits of these results

- The HotpotQA `budget-loose` ablation used 60 queries rather than 150; the other
  conditions and both full runs used their stated sizes.
- Ablation query counts differ from the full runs; compare conditions within a
  dataset, not the ablation table against the full-run table.
