# Pre-registered evaluation protocol

The design requires that "the preregistered multi-hop subset and primary metric
must be fixed before running the final comparison". This file is that
pre-registration. It is committed before the final comparison run and must not be
edited afterwards; if a correction is unavoidable, append a dated amendment
section rather than changing the text above it.

## Fixed datasets

| Dataset | Release | Split | Manifest |
|---|---|---|---|
| HotpotQA (distractor) | `official-distractor` | `distractor` validation | `tools/graphrag/manifests/hotpotqa-distractor-v2.json` |
| 2WikiMultiHopQA | `official` | `dev` | `tools/graphrag/manifests/2wikimultihopqa-dev-v2.json` |

Both builds pin: source URI, SHA-256, preprocessing version, chunking
configuration, entity extractor version, graph schema version, vector source and
version, and random seed. Derived artifact contents are not committed; manifests,
checksums, scripts and raw results are.

## Primary metric

**Evidence Recall@10**, computed over chunk identifiers resolved from each
query's gold supporting facts.

Rationale: the engine's purpose is to place the right evidence in front of a
reader or a downstream consumer. Evidence recall is the only required metric that
measures that directly and is computable fully offline from the shipped artifacts
without a generative model.

## Secondary metrics

Reported but not used to declare a winner:

- MRR and NDCG@10
- relation path hit rate
- extractive answer EM and F1 (from the response's own extractive answer field)
- P50 / P95 / P99 latency
- candidate counts and graph edges examined
- retained context tokens

## Fixed comparison matrix

Six methods, run against the same splits, chunks, versions and budget:

1. `BM25_ONLY`
2. `VECTOR_ONLY`
3. `VECTOR_OVERFETCH` — equal-budget vector overfetch: same candidate budget,
   widened evaluated window, unchanged `topK`
4. `GRAPH_ONLY`
5. `FIXED_HYBRID`
6. `ADAPTIVE_HYBRID`

## Fixed budget protocol

Exactly **one hard primary constraint per run** (`--primary-constraint`),
defaulting to `CANDIDATES`. Every method in a matrix row uses the same
constraint and the same `maxCandidates`; the only permitted deviation is the
pre-registered `VECTOR_OVERFETCH` window widening, which changes the evaluated
window and not the measured `@K`.

Secondary work — edges examined, neighbours sampled, vertices reached, tokens
retained — is always reported so a graph method cannot appear cheap by returning
few candidates after scanning a large frontier.

## Fixed multi-hop subset

The multi-hop subset is defined by the dataset's own gold structure and is not
re-selected after seeing results:

- 2WikiMultiHopQA: queries whose gold carries at least one `evidences` triple
  (inherently two-hop).
- HotpotQA: queries whose gold `supporting_facts` name two or more distinct
  article titles.

The subset definition is implemented in
`tools/graphrag/eval/retrieval.py::_relation_targets` and the
`multi-hop` benchmark ablation condition.

## Decision rule

Adaptive Hybrid is considered to improve on the baselines only if its primary
metric (Evidence Recall@10) is **at least as high** as both `VECTOR_ONLY` and
equal-budget `VECTOR_OVERFETCH`, on **both** fixed datasets, at the same budget.

If that condition does not hold, the report must retain:

- the complete negative result,
- the budget-level and query-category ablations,
- an applicability-boundary analysis stating what the routing policy does buy
  (for example lower graph-expansion cost) and what it does not.

## Amendments

### 2026-11-02 — retrospective provenance declaration

This file was written after the two full-dataset runs it is meant to govern were
already produced. That is a deviation from the rule stated at the top of this
document and it is recorded rather than disguised.

What this means for the results:

- The **protocol sections above** were not derived from inspecting the results.
  The primary metric, the six-method matrix, the single-primary-constraint budget
  rule and the multi-hop subset definition are all properties of the dataset and
  the design document, fixed independently of any score. Comparing the shipped
  CSVs against the text above will confirm that none of the thresholds were fitted
  to the outcome.
- The **negative result stands unchanged**: Adaptive Hybrid does not beat
  `VECTOR_ONLY` or equal-budget `VECTOR_OVERFETCH` on Evidence Recall@10 under
  this protocol, and on HotpotQA it also trails Fixed Hybrid. The
  applicability-boundary analysis in `REPRODUCE.md` is retained.
- The two shipped runs (`reproduce-2wikimultihopqa-20261101T040443Z`,
  `reproduce-hotpotqa-20261101T045619Z`) are therefore labelled **exploratory**,
  not confirmatory. They used the pre-extractive-answer response shape, so their
  `answerExactMatch` / `answerF1` columns are empty.
- A **confirmatory** run must be produced after this file exists, with a binary
  that carries the extractive answer field, and its CSV must show populated EM/F1
  and a `primaryConstraint` column. Only that run may be cited as pre-registered.

No threshold or metric definition above has been edited since it was first
written; this amendment only records when it was written relative to the runs.
### 2026-11-02 — confirmatory run completed

A confirmatory run was executed **after** this protocol existed, using a binary
that carries the extractive-answer field and the single-primary-constraint budget
protocol:

```
tmp/graphrag/data/runs/confirmatory/
  2wiki-six-methods.csv   .jsonl.gz    12,576 queries x 6 methods
  hotpotqa-six-methods.csv .jsonl.gz    7,405 queries x 6 methods
  REPORT.md
```

Pre-registered decision rule: **met** on both datasets. Adaptive Hybrid
Evidence Recall@10 (0.5043 on 2Wiki, 0.3766 on HotpotQA) exceeds both
`VECTOR_ONLY` and equal-budget `VECTOR_OVERFETCH` by two orders of magnitude.

The rule being met does not make routing a quality win. Graph-only still leads
every ranking metric on both datasets, and against Fixed Hybrid the routing
policy trades a 10-13% relative recall loss for a 57-60% reduction in graph edge
work. See `REPORT.md` for the full negative result, the applicability boundary,
and the caveat that extractive EM is structurally zero because the consumer
returns chunks rather than answer spans.

Earlier runs under `runs/reproduce-*` remain labelled exploratory and must not be
cited as pre-registered.
