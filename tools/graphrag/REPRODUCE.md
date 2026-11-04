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

Both fixed datasets were run end to end at full size with the current runner. Every
request returned 200, and EM/F1 are produced through `POST /api/v1/answers`.

### 2WikiMultiHopQA dev - 12,576 queries x 6 methods (75,456 requests)

Artifact: `tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261104T045732Z/`
(1.1 GB `jsonl.gz`, 54 CSV columns)

| Method | Recall@10 | MRR | NDCG@10 | EM | F1 | p50 ms | p95 ms | edges | tokens | abstentions |
|---|---|---|---|---|---|---|---|---|---|---|
| BM25_ONLY | 0.0021 | 0.0037 | 0.0024 | 0.0000 | 0.0011 | 9.3 | 18.6 | 0 | 2723 | 0 |
| VECTOR_ONLY | 0.0020 | 0.0034 | 0.0023 | 0.0000 | 0.0010 | 7.6 | 16.9 | 0 | 2474 | 0 |
| VECTOR_OVERFETCH | 0.0079 | 0.0135 | 0.0090 | 0.0000 | 0.0011 | 11.1 | 19.1 | 0 | 2635 | 0 |
| GRAPH_ONLY | **0.5441** | **0.5909** | **0.5050** | 0.0000 | 0.0086 | 34.2 | 54.5 | 99.5 | 841 | 63 |
| FIXED_HYBRID | 0.4282 | 0.0857 | 0.1681 | 0.0000 | 0.0018 | 36.5 | 60.9 | 78.7 | 2635 | 0 |
| ADAPTIVE_HYBRID | 0.4353 | 0.0969 | 0.1768 | 0.0000 | 0.0018 | 42.7 | 73.1 | 39.7 | 2646 | 0 |

### HotpotQA distractor validation - 7,405 queries x 6 methods (44,430 requests)

Artifact: `tmp/graphrag/data/runs/reproduce-hotpotqa-20261104T050902Z/`
(576 MB `jsonl.gz`). This run is the first HotpotQA full run whose evidence-quality
columns are populated: earlier runs silently lost the gold file (see "What the earlier
ablation numbers got wrong", item 3).

| Method | Recall@10 | MRR | NDCG@10 | EM | F1 | p50 ms | p95 ms | edges | tokens | abstentions |
|---|---|---|---|---|---|---|---|---|---|---|
| BM25_ONLY | 0.0025 | 0.0048 | 0.0030 | 0.0000 | 0.0009 | 9.7 | 18.2 | 0 | 2883 | 0 |
| VECTOR_ONLY | 0.0003 | 0.0001 | 0.0001 | 0.0000 | 0.0007 | 8.0 | 15.5 | 0 | 2777 | 0 |
| VECTOR_OVERFETCH | 0.0001 | 0.0001 | 0.0001 | 0.0000 | 0.0007 | 11.7 | 18.5 | 0 | 2823 | 0 |
| GRAPH_ONLY | **0.4052** | **0.3611** | **0.3322** | 0.0000 | 0.0081 | 26.1 | 39.0 | 74.2 | 702 | **1865** |
| FIXED_HYBRID | 0.3246 | 0.0595 | 0.1238 | 0.0000 | 0.0015 | 32.6 | 53.4 | 81.2 | 2673 | 0 |
| ADAPTIVE_HYBRID | 0.3273 | 0.0587 | 0.1242 | 0.0000 | 0.0015 | 36.4 | 56.4 | 35.1 | 2698 | 0 |

### Note on EM and F1

`answerExactMatch` is 0.0000 for every method on both datasets and `answerF1` never
exceeds 0.0086. These columns are now genuinely populated (the runner calls
`/api/v1/answers` and scores the returned extractive answer) rather than empty, so the
values are a real measurement of the extractive consumer: it returns whole cited chunks,
not answer spans, so exact match against a short gold answer such as `yes` or a date is
structurally unreachable. Treat EM/F1 as a lower bound on overlap, not as answer
accuracy, and do not cite them as evidence of answer quality either way.

`GRAPH_ONLY` abstains on 1,865 of 7,405 HotpotQA queries and 63 of 12,576 2Wiki queries.
An abstention is reported in `answerAbstentionCount`; it is an outcome of the answer
layer, not a missing metric, and those queries are excluded from the EM/F1 mean rather
than scored as empty strings.

## Status of Adaptive Hybrid

**The original run was a negative result and it is retained as one.** On both datasets
Graph-only led every quality metric: Adaptive Hybrid's MRR was an order of magnitude below
graph-only (0.1534 vs 0.6344 on 2Wiki) because the routing policy weighted the text channels
above the graph channel and gave the graph channel 16 of 50 candidates, while BM25 and vector
recall sat at 0.002.

That was a routing defect, not a property of adaptive retrieval. It was traced to hard-coded
fusion weights (text 1.25 vs graph 1.0) and a candidate split that starved the graph channel,
and both are now configuration (`retrieval.graph-fusion.*`). After the fix, on a fixed
500-query slice:

| Dataset | Graph-only MRR | Adaptive MRR (before) | Adaptive MRR (after) | Adaptive edges |
|---|---|---|---|---|
| 2WikiMultiHopQA | 0.5445 | 0.1019 | 0.4747 | 33.1 (graph-only 45.9) |
| HotpotQA | 0.3327 | 0.0723 | 0.2910 | 27.7 (graph-only 20.4) |

Adaptive Hybrid now beats Fixed Hybrid on MRR by roughly 4-5x and approaches graph-only while
doing less graph work on 2Wiki.

**Remaining honest caveat**: Adaptive Hybrid still does not exceed graph-only. Fusion keeps
contributing the text channels' rank terms even though those channels carry almost no gold signal,
which is the residual gap (2Wiki 0.4747 vs 0.5445). Treat the routing policy as a cost-control
mechanism that preserves most of the graph channel's quality, not yet as a quality lever.

`tools/graphrag/tests/test_adaptive_quality.py` encodes the measured baseline and fails if the
adaptive channel regresses toward the text-dominated behaviour or stops clearly beating fixed
hybrid, so this section cannot quietly become stale again.

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

ADAPTIVE_HYBRID and FIXED_HYBRID under each condition. The table below reports the
**re-measured** values (120 queries per dataset) taken after the ablation wiring was
fixed; see the next subsection for what was wrong with the earlier numbers.

All values are ADAPTIVE_HYBRID, 120 queries per dataset, every request 200.

| Condition | Candidate budget | Mode sent | 2Wiki Recall / MRR | HotpotQA Recall / MRR | Edges |
|---|---|---|---|---|---|
| budget-loose | 400 | unchanged | 0.385 / 0.072 | 0.324 / 0.060 | 24-26 |
| multi-hop | 50 | unchanged | 0.385 / 0.072 | 0.324 / 0.060 | 24-26 |
| (reference, full run) | 100 | unchanged | 0.435 / 0.097 | 0.327 / 0.059 | 30-35 |
| budget-tight | 25 | unchanged | 0.331 / 0.058 | 0.251 / 0.041 | 19-23 |
| high-degree | 25 | `GRAPH_ONLY` | 0.497 / 0.549 | 0.408 / 0.336 | 23-43 |
| no-anchor | 100 | `BM25_ONLY` | 0.002 / 0.008 | 0.000 / 0.000 | 0 |

**Reading caution: the candidate-budget ablations barely move the graph channel.**
`DeterministicRulePolicy.neighborCap` returns `min(baseNeighborCap, candidateBudget)` in
the normal branch (`baseNeighborCap` ships as 16), and `RecallService` then applies
`min(candidateBudget, plan.neighborCap)` again. Any candidate budget of 16 or more
therefore collapses to the same 16-neighbour window. That is why `multi-hop` (50) and
`budget-loose` (400) produce identical metrics on both datasets: the budget lever never
reaches the graph stage in those rows.

Only `budget-tight` (25) and lower actually shrink the window, and it lowers recall by
roughly 0.05-0.07. Do not read the `budget-loose` / `multi-hop` rows as evidence that a
larger budget helps: under the current plan builder a larger budget is simply not
propagated. Raising `baseNeighborCap` is the knob that changes them, and these ablations
do not exercise it.

### What the earlier ablation numbers got wrong

The pre-fix ablation artifacts reported `high-degree` and `no-anchor` as 0.000 recall
on both datasets, and the report read that as "the graph channel is the only evidence
source". Three separate defects produced that reading, and all of them are fixed:

1. **The CSV recorded the pre-ablation mode.** A `BM25_ONLY` request appeared in the
   `mode` column as `FIXED_HYBRID`/`ADAPTIVE_HYBRID`. The runner now writes the executed
   mode in `mode` and keeps the method identity in `requestedMode`;
   `test_ablation_rows_record_the_mode_actually_sent` pins it.

2. **Every ablated request was rejected with HTTP 400.** The hybrid body carries a
   `queryVector`, and `BM25_ONLY`/`GRAPH_ONLY` answer it with
   `UNSUPPORTED_OPTION: queryVector is not supported for mode ...`. All 240 requests
   per condition failed, and the runner's retry loop recorded the failures as rows, so
   the summary averaged a set of non-responses into `recall 0.0` and `meanEvaluated*=0`.
   `apply_ablation` now drops `queryVector` (and `exactRanking`) when it rewrites the
   mode to a single-channel one; `test_single_channel_ablations_drop_the_query_vector`
   pins it.

3. **`reproduce.sh` silently dropped HotpotQA gold.** Its cache lives at
   `cache/hotpotqa-hf/`, and the old `ls <dir> <dir>/*` discovery yielded a directory
   header line with a trailing colon that `-e` rejects. Every HotpotQA evidence-quality
   column was therefore empty while the run looked successful. Discovery now uses
   `find` and prefers a readable gold file inside a cache directory.

Consequences for the claims:

- **`high-degree` is not zero.** It measures 0.497 (2Wiki) and 0.408 (HotpotQA), which is
  *higher* than the reference hybrid configuration on both datasets. The neighbour cap is
  not suppressing graph evidence; it is trading recall for a smaller frontier.
- **`no-anchor` is genuinely near zero, and now it is measured rather than idle.** The
  BM25 channel evaluates 100 candidates per query (`meanEvaluatedBM25 = 100`) and still
  returns recall 0.002 / 0.000. That matches the standalone `BM25_ONLY` baseline in the
  main table, so this row restates the single-channel baseline instead of testing a
  substitution hypothesis.
- **The budget story is weaker than it looks.** `budget-tight` (recall 0.331/0.251) is
  below `budget-loose` (0.385/0.324), but `multi-hop` and `budget-loose` are identical,
  because the adaptive plan's 16-neighbour cap dominates any candidate budget of 16 or
  more. The only real effect measured here is that a window *smaller* than the cap hurts.
  A wider window is never exercised, so this run cannot support a "more budget, more
  recall" claim about the graph stage.

The ablation harness itself is covered by `test_budget_ablations_scale_the_wire_budget_only`
and `test_reported_budget_reflects_the_ablated_request`, which pins that an ablation
scales only the budget and that the recorded budget is the executed one.

## Confirmatory run

The pre-registered comparison is
`tmp/graphrag/data/runs/confirmatory/REPORT.md`, with per-request results in
`2wiki-six-methods.*` and `hotpotqa-six-methods.*` alongside it. It was produced
after `PREREGISTRATION.md` existed, records `primaryConstraint` on every row, and
has populated `answerExactMatch`/`answerF1`.

The older `runs/reproduce-*` artifacts predate the extractive-answer field and the
budget-constraint column. They are retained as exploratory evidence only.

## Retrieval engine

`reproduce.sh` can drive retrieval two ways:

```bash
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa             # engine=http (default)
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa --engine batch
```

Both use the same Java retrieval code. `--engine batch` calls
`RetrievalBatchCli`, which the design names as the batch-mode entry point: policy,
budget accounting and trace generation stay in Java, so a batch run and a served
run cannot silently diverge. Batch mode writes raw JSONL and leaves scoring to
the evaluation step.

## Metric availability

Every required metric is now reported by the runner. Two columns depend on inputs
being present, and this section states the dependency explicitly so a future run
is not misread:

| Column | Populated when | Notes |
|---|---|---|
| `evidenceRecallAtK`, `mrr`, `ndcgAtK` | `--gold` and `--chunks` are supplied | chunk-id level recall |
| `answerExactMatch`, `answerF1` | `--answers-url` is supplied **and** the query carries a gold `answer` | `/api/v1/retrievals` never returns an `answer` field, so without `--answers-url` these columns stay empty by design. The runner then calls `POST /api/v1/answers` per retrieval; an abstention is recorded in `answerAbstentionCount` instead of being scored as an empty string. Extraction returns whole chunks, so EM against a short gold answer is structurally near-zero: treat these as a lower bound, not as answer accuracy |
| `relationPathHit`, `relationPathTargetCount` | gold carries relation targets | 2Wiki: explicit `evidences`. HotpotQA: derived from supporting-fact titles, since it is not relation-annotated. `relationPathTargetCount` separates "no gold path to score" from "path missed", which a bare `0` cannot |
| `primaryConstraint` | always | the ONE hard budget constraint this run spent |
| `meanNeighborsSampled`, `meanVerticesReached`, `meanUniqueEvidenceCandidates` | always | secondary work, so a graph method cannot look cheap by returning few candidates |
| `policyParameters`, `featureSchemaVersion` | adaptive runs | together with `policyId`/`policyVersion`, these are what make a routing decision replayable |
| `mode`, `requestedMode` | always | `mode` is what the server received; `requestedMode` is the method's own mode. They differ under an ablation, which rewrites the wire mode |

`VECTOR_OVERFETCH` is an equal-budget baseline: it keeps `topK` (and therefore
the measured `@K`) identical to every other method and widens only the evaluated
candidate window. Compare it against `VECTOR_ONLY` at the same
`--primary-constraint`.

The fixed protocol that must be decided before a final comparison is recorded in
[`PREREGISTRATION.md`](./PREREGISTRATION.md).

## Known limits of these results

- The HotpotQA `budget-loose` ablation used 60 queries rather than 150; the other
  conditions and both full runs used their stated sizes.
- Ablation query counts differ from the full runs; compare conditions within a
  dataset, not the ablation table against the full-run table.
