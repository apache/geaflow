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
| `<dataset>-six-methods.jsonl` | Per-request record: status, evidence, trace, metrics |
| `<dataset>-six-methods.csv` | Per-method summary: Recall@K, MRR, NDCG, path hit, EM/F1, P50/P95/P99, candidate/edge/token cost, policy id, code revision, seed |
| `ready.json` | Service readiness captured at startup |
| `service.log` | Service stdout/stderr |

## Measured example

A two-query smoke run against the published 2Wiki bundle produced a CSV with
populated `evidenceRecallAtK`, `mrr`, `ndcgAtK`, `meanEdgesExamined` and
`meanTokensRetained`, and confirmed that `ADAPTIVE_HYBRID` reports
`policyId=deterministic-rule` while `FIXED_HYBRID` does not — the routing policy
is only consulted for the adaptive method.

## Limits

A full-dataset run and the long production load profile need the complete local
cache and a host sized for the artifact; this repository only ships the smoke
evidence produced in-session. Record the `codeRevision` column with any result
you publish.
