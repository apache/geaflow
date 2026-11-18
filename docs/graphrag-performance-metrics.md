# GraphRAG Retrieval: Performance Metrics

This report covers the five retrieval methods on two fixed datasets, reporting quality
and cost together. Every figure comes from a CSV under `tmp/graphrag/data/runs/`; the run
directories are named in section 7. Reproduction steps are in
`graphrag-external-reproduction.md`.

## 1. Headline

Adaptive hybrid routing does not beat fixed hybrid on ranking quality, and beats
graph-only by a wide margin only because graph-only is a weak baseline on these corpora.
The honest summary:

| Question | Answer |
| --- | --- |
| Is adaptive better than graph-only? | Yes on every ranking metric, by 36-150%, but graph-only is the worst non-degenerate baseline |
| Is adaptive better than fixed hybrid? | No. It is 5-13% worse on MRR and NDCG on both datasets |
| Is adaptive cheaper than graph-only? | Only on 2Wiki (34% fewer edges). On HotpotQA it costs 65% more |
| Does any method answer the question? | No. F1 is at most 0.0095 and EM is 0 everywhere |

The last row matters most and is discussed in section 5.

## 2. Configuration

| Setting | Value |
| --- | --- |
| Queries | first 500 of each dev split, deterministic prefix |
| `topK` | 10 |
| `maxCandidates` | 100 |
| Primary constraint | `CANDIDATES` |
| `exactRanking` | true (full index scan before ranking) |
| Execution mode | as configured per method |
| Embeddings | precomputed, `gte` vectors, from the published bundle |
| Token estimator | `chars4-v1` (characters / 4) |
| Seed | 0 |

## 3. Quality

### 3.1 2WikiMultiHopQA, 500 queries

| Method | Recall@10 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.7368 | 0.6979 | 0.0023 | 0.0000 |
| VECTOR_ONLY | 0.7142 | 0.7154 | 0.6406 | 0.0024 | 0.0000 |
| VECTOR_OVERFETCH | 0.7142 | 0.7154 | 0.6406 | 0.0024 | 0.0000 |
| GRAPH_ONLY | 0.5030 | 0.5260 | 0.4506 | 0.0095 | 0.0000 |
| FIXED_HYBRID | **0.8435** | **0.7703** | **0.7388** | 0.0024 | 0.0000 |
| ADAPTIVE_HYBRID | 0.8358 | 0.7182 | 0.6983 | 0.0024 | 0.0000 |

### 3.2 HotpotQA distractor, 500 queries

| Method | Recall@10 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.8254 | 0.8235 | 0.0025 | 0.0000 |
| VECTOR_ONLY | 0.8197 | 0.7658 | 0.7121 | 0.0024 | 0.0000 |
| VECTOR_OVERFETCH | 0.8197 | 0.7658 | 0.7121 | 0.0024 | 0.0000 |
| GRAPH_ONLY | 0.3818 | 0.3272 | 0.3056 | 0.0079 | 0.0000 |
| FIXED_HYBRID | 0.9460 | **0.8008** | **0.7914** | 0.0025 | 0.0000 |
| ADAPTIVE_HYBRID | **0.9525** | 0.6970 | 0.7323 | 0.0025 | 0.0000 |

Bold marks the best value per column.

## 4. Adaptive versus the single-path baselines

Requested comparison against each single-path method. Absolute delta and relative
percentage; negative values are reported as they are.

### 4.1 2WikiMultiHopQA

| Metric | ADAPTIVE | GRAPH_ONLY | Δ vs graph | FIXED_HYBRID | Δ vs fixed |
| --- | --- | --- | --- | --- | --- |
| Recall@10 | 0.8358 | 0.5030 | **+0.3328 (+66.2%)** | 0.8435 | -0.0077 (-0.9%) |
| MRR | 0.7182 | 0.5260 | **+0.1922 (+36.5%)** | 0.7703 | -0.0522 (-6.8%) |
| NDCG@10 | 0.6983 | 0.4506 | **+0.2476 (+54.9%)** | 0.7388 | -0.0406 (-5.5%) |
| F1 | 0.0024 | 0.0095 | -0.0071 (-74.8%) | 0.0024 | -0.0000 (-0.6%) |
| Edges examined | 30.1 | 45.9 | -15.8 (-34.4%) | 36.3 | -6.2 (-17.0%) |
| Tokens retained | 2682 | 842 | +1841 (+218.6%) | 2677 | +6 (+0.2%) |
| P95 latency (ms) | 2905 | 1248 | +1656 (+132.7%) | 2867 | +38 (+1.3%) |

### 4.2 HotpotQA distractor

| Metric | ADAPTIVE | GRAPH_ONLY | Δ vs graph | FIXED_HYBRID | Δ vs fixed |
| --- | --- | --- | --- | --- | --- |
| Recall@10 | 0.9525 | 0.3818 | **+0.5707 (+149.5%)** | 0.9460 | +0.0065 (+0.7%) |
| MRR | 0.6970 | 0.3272 | **+0.3698 (+113.0%)** | 0.8008 | -0.1038 (-13.0%) |
| NDCG@10 | 0.7323 | 0.3056 | **+0.4267 (+139.6%)** | 0.7914 | -0.0591 (-7.5%) |
| F1 | 0.0024 | 0.0079 | -0.0054 (-68.7%) | 0.0024 | +0.0000 (+0.6%) |
| Edges examined | 33.7 | 20.4 | +13.2 (+64.9%) | 38.4 | -4.8 (-12.4%) |
| Tokens retained | 2837 | 658 | +2179 (+331.1%) | 2828 | +10 (+0.3%) |
| P95 latency (ms) | 1909 | 499 | +1410 (+282.7%) | 1887 | +23 (+1.2%) |

### 4.3 Reading the comparison honestly

Three conclusions follow directly from the tables.

**Against graph-only, the gain is large but the baseline is weak.** Graph-only scores 0.50
recall on 2Wiki and 0.38 on HotpotQA, well below both text channels. Quoting "+66%" or
"+150%" without noting that graph-only is the worst non-degenerate method overstates the
result.

**Against fixed hybrid, adaptive loses on ranking.** MRR is 6.8% lower on 2Wiki and 13.0%
lower on HotpotQA. Adaptive wins Recall@10 on HotpotQA by 0.7%, which is within noise for
500 queries. The routing policy currently costs ranking quality relative to the simpler
fixed fusion.

**Cost savings appear on only one dataset.** Adaptive scans 34% fewer edges than
graph-only on 2Wiki but 65% more on HotpotQA, and it carries 2-3x the tokens of graph-only
on both because it keeps the text channels' evidence alongside the graph paths.

## 5. Cost and the answer layer

### 5.1 Token cost, measured

`meanTokensRetained` is the estimated token count of the evidence actually carried into
the answer layer, computed with `chars4-v1` (characters divided by four, rounded up, floor
of one). It is a retrieval-stage figure, not an LLM billing figure.

| Method | 2Wiki mean | 2Wiki total (500) | HotpotQA mean | HotpotQA total (500) |
| --- | --- | --- | --- | --- |
| BM25_ONLY | 2806 | 1,402,850 | 2854 | 1,427,150 |
| VECTOR_ONLY | 2603 | 1,301,400 | 2779 | 1,389,550 |
| GRAPH_ONLY | 842 | 420,930 | 658 | 329,090 |
| FIXED_HYBRID | 2677 | 1,338,250 | 2828 | 1,413,850 |
| ADAPTIVE_HYBRID | 2682 | 1,341,200 | 2837 | 1,418,700 |

`chars4-v1` is an approximation. For English text it tends to run slightly low against
BPE tokenizers; treat these as relative comparisons between methods rather than absolute
token counts.

### 5.2 Token cost, if an LLM wrote the answer

The retrieval layer returns spans; it does not call a model. If a deployment attached one,
the per-query cost would follow from the retained tokens:

```
prompt_tokens  = system_prompt + question + meanTokensRetained
cost_per_query = prompt_tokens * prompt_price + answer_tokens * completion_price
```

Worked example at an illustrative \$0.15 / 1M prompt tokens and \$0.60 / 1M completion
tokens, with a 200-token system prompt, 30-token question and 50-token answer:

| Method | Prompt tokens | Cost per 1k queries |
| --- | --- | --- |
| GRAPH_ONLY | 1092 | \$0.19 |
| FIXED_HYBRID | 2945 | \$0.47 |
| ADAPTIVE_HYBRID | 2949 | \$0.47 |
| BM25_ONLY | 3071 | \$0.49 |

Pricing is deliberately parameterised: substitute the current rate for the model in use.
The relative relationship is what matters - graph-only carries roughly one third the
prompt cost, and adaptive carries the same as fixed hybrid because both keep text
evidence.

### 5.3 Why F1 is near zero for every method

All five methods land between 0.0022 and 0.0095, and EM is 0 everywhere. This is a
property of the answer layer, not of retrieval:

- The answer consumer is **extractive**: it returns a span from a retrieved chunk.
- Gold answers are short entities (`Małgorzata Braunek`, `yes`, `2`).
- Retrieved chunks are entity-name lists or multi-sentence paragraphs, so the extractor
  rarely emits exactly the gold string.

The one exception is informative: GRAPH_ONLY has the **highest** F1 (0.0095 on 2Wiki,
0.0079 on HotpotQA) while having the **lowest** recall. Its narrow, entity-dense evidence
makes span extraction slightly more likely to hit a short gold answer. Recall and F1 are
therefore not measuring the same thing here, and F1 should not be used to rank these
methods. A meaningful F1 requires a generative answer model, which this module does not
include.

## 6. Latency

| Method | 2Wiki P50 | 2Wiki P95 | 2Wiki P99 | HotpotQA P50 | HotpotQA P95 | HotpotQA P99 |
| --- | --- | --- | --- | --- | --- | --- |
| VECTOR_ONLY | 1050 | 1132 | 1240 | 620 | 667 | 731 |
| VECTOR_OVERFETCH | 880 | 945 | 1020 | 505 | 548 | 610 |
| GRAPH_ONLY | 1120 | 1248 | 1390 | 460 | 499 | 552 |
| FIXED_HYBRID | 2700 | 2867 | 3105 | 1750 | 1887 | 2040 |
| ADAPTIVE_HYBRID | 2740 | 2905 | 3150 | 1770 | 1909 | 2065 |

Hybrid methods cost 2-3x a single channel because `exactRanking` scans the full text
index. Adaptive adds roughly 1-2% over fixed hybrid. Latency figures are machine-specific
and should be re-measured on the target host.

## 7. Provenance

| Dataset | Run directory |
| --- | --- |
| 2WikiMultiHopQA | `tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261112T074834Z` |
| HotpotQA | `tmp/graphrag/data/runs/reproduce-hotpotqa-20261112T081416Z` |

Summary files: `<run>/<dataset>-six-methods.csv`. Per-request records:
`<run>/<dataset>-six-methods.jsonl.gz`.

Both runs used `codeRevision` from this branch, `datasetRelease` `e9b966b931bee494`
(2Wiki) / `1f41fcdab9da26b5` (HotpotQA), `tokenEstimatorVersion` `chars4-v1`.

## 8. What this report does not establish

- No claim about retrieval quality on other corpora. Two multi-hop English benchmarks do
  not generalise to other languages or domains.
- No claim that the answer layer is useful. F1 near zero means it is not, as configured.
- No production capacity claim. These are single-process local runs without concurrency.
- No tuned configuration. Every parameter is at its shipped default; no sweep was run.

The adaptive routing analysis, including why it does not beat fixed hybrid, is in
`graphrag-adaptive-ablation.md`.