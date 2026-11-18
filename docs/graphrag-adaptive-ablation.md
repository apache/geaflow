# GraphRAG Adaptive Routing: Component Ablation and Limitations

This report answers two questions the earlier material left open:

1. Which decision components in the adaptive routing module actually affect the result?
2. Why does adaptive routing not reliably beat the single-path baselines?

The second answer is uncomfortable and is stated plainly in section 5: on 2WikiMultiHopQA
the routing policy's main configured behaviour - favouring the graph channel - **makes
ranking worse**, and neutralising it improves MRR by 11%.

## 1. What the existing ablations could and could not show

`benchmark_http.py --ablation` implements eight *scenario* ablations: `no-anchor`,
`high-degree`, `lexical`, `semantic`, `multi-hop`, `budget-tight`, `budget-loose`. Each
one reshapes the request - changing the mode, or dividing the candidate budget.

They answer "what happens if this channel or this budget is absent", which is useful, but
they cannot attribute a result to a *decision component*. Routing has seven configurable
inputs; changing the mode bypasses all of them at once, so a delta cannot be assigned to
any single rule.

Section 2 adds the missing ablation.

## 2. Component-level ablation design

Each condition restarts the service with exactly **one** routing setting overridden, then
runs `FIXED_HYBRID` and `ADAPTIVE_HYBRID` over the same 200 queries. Because only one
environment variable changes between conditions, a delta is attributable to that
component.

| Condition | Override | Neutralises |
| --- | --- | --- |
| `baseline` | none | - (shipped defaults) |
| `no-router` | `graph-weight=1.0`, `text-weight=1.0` | graph preference in fusion |
| `no-graph-fusion` | `graph-weight=0.25` | graph ranked like a text channel |
| `no-replan` | `max-replan-count=0` | mid-request budget reshaping |
| `no-probe-share` | `probe-share=0.5` | text probe's budget reservation |
| `no-idf-trigger` | `high-average-idf=1000` | lexical-confidence rule |
| `no-degree-trigger` | `high-candidate-degree=1000000` | hub-vertex rule |
| `no-cascade-gate` | `min-probe-evidence=1`, `min-probe-score-gap=0` | cascaded-stage gate |

Two notes on the last row: `min-probe-evidence` cannot be `0` because the configuration
validator rejects it, so the gate is widened rather than removed. This is the validator
working correctly, not a limitation of the experiment.

Reproduce with `tools/graphrag/ablate_adaptive.sh` (see
`graphrag-external-reproduction.md`, section 4.3).

## 3. Results

2WikiMultiHopQA, 200 queries, ADAPTIVE_HYBRID. Deltas are against `baseline`.

| Condition | Recall@10 | Δ Recall | MRR | Δ MRR | Edges | Replan | Tokens |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 0.8392 | - | 0.6796 | - | 30.1 | 1.21 | 2682 |
| no-router | 0.8454 | +0.0063 | **0.7562** | **+0.0765** | 30.1 | 1.21 | 2684 |
| no-graph-fusion | 0.8454 | +0.0063 | **0.7562** | **+0.0765** | 30.1 | 1.21 | 2684 |
| no-cascade-gate | 0.8467 | +0.0075 | 0.6841 | +0.0044 | 38.1 | 1.64 | 2674 |
| no-probe-share | 0.8400 | +0.0008 | 0.6890 | +0.0093 | 30.4 | 1.24 | 2684 |
| no-replan | 0.8392 | +0.0000 | 0.6796 | +0.0000 | 30.1 | **0.00** | 2682 |
| no-idf-trigger | 0.8392 | +0.0000 | 0.6796 | +0.0000 | 30.1 | 1.21 | 2682 |
| no-degree-trigger | 0.8392 | +0.0000 | 0.6796 | +0.0000 | 30.1 | 1.25 | 2682 |

FIXED_HYBRID is unaffected by every condition except `no-cascade-gate`, which is the
control: it confirms the overrides reach the adaptive path and nothing else.

| Condition | FIXED MRR | FIXED edges |
| --- | --- | --- |
| baseline | 0.7506 | 34.6 |
| no-cascade-gate | 0.7517 | 47.3 |
| all others | 0.7506 | 34.6 |

## 4. What the ablation shows

**Four of seven components have zero measurable effect.** `no-replan`,
`no-idf-trigger`, `no-degree-trigger` produce bit-identical results to baseline. The
replan counter does drop to 0.00, so the override took effect - the policy simply reaches
the same final plan whether or not it is allowed to revise it.

**Graph preference actively hurts.** `no-router` and `no-graph-fusion` both raise MRR from
0.6796 to 0.7562 (+11.3%). They are the same condition seen from two directions: both
flatten the graph's advantage in fused ranking. With `graph-weight` at the shipped 2.0
against `text-weight` 0.25, the graph's reciprocal-rank contribution is weighted eight
times higher than text, and on 2Wiki the text channels are the stronger retriever
(BM25 MRR 0.7368 vs graph 0.5260). Weighting the weaker channel up costs ranking quality.

**The cascade gate costs some edges without buying quality.** Widening it
(`no-cascade-gate`) raises edges from 30.1 to 38.1 (+26.6%) and replans from 1.21 to 1.64,
for +0.0044 MRR - which is within noise for 200 queries.

**Replanning is not doing useful work.** It fires on average 1.21 times per request and
changes nothing measurable. This is the clearest single finding: a component that
consumes budget and adds code path without affecting output.

## 5. Why adaptive does not reliably beat the single-path baselines

The ablation makes the mechanism visible. There are four reasons, in order of measured
impact.

### 5.1 The fusion weighting is inverted for these corpora

Adaptive's defining behaviour is to weight graph evidence above text. That is the right
prior for a graph-retrieval system, but on both datasets the text channels retrieve
better than the graph channel:

| Dataset | BM25 MRR | GRAPH_ONLY MRR | Gap |
| --- | --- | --- | --- |
| 2WikiMultiHopQA | 0.7368 | 0.5260 | text leads by 0.2108 |
| HotpotQA | 0.8254 | 0.3272 | text leads by 0.4982 |

With `graph-weight=2.0` and `text-weight=0.25`, reciprocal-rank fusion promotes graph
results that rank behind text results. Section 3 measures the consequence: flattening the
weights improves MRR by 11.3%.

This is not a bug in the implementation - the weighting does exactly what it is configured
to do. It is a mismatch between the policy's prior and the data.

### 5.2 The confidence rules never fire

`high-average-idf` defaults to 1.5, but the observed median `average_idf` is 3.01. The
rule cannot trigger. Raising the threshold to 1000 (`no-idf-trigger`) changes nothing,
which confirms the rule was already dormant. The same holds for `high-candidate-degree`.

A rule that can never fire is dead weight: it appears in the policy, is recorded in every
trace, and contributes nothing.

### 5.3 Replanning revises a plan that does not need revising

`max-replan-count=1` permits one mid-request revision. It executes 1.21 times per request
and changes no outcome. The budget it reallocates is evidently not the binding constraint
at these query sizes.

### 5.4 The decision is a fixed rule table, not an optimisation

`DeterministicRulePolicy` maps feature-threshold pairs to plans. It was written to be
reproducible and auditable, and it is both. It was **not** fitted to any data: every
threshold is a hand-chosen default, and no sweep has been run against either corpus.

The consequence is measurable: the policy's dominant decision (favour graph) is wrong for
these datasets, and three of its seven rules never fire. A policy fitted on held-out data
would very likely set `graph-weight` below 1.0, or disable the graph path when the text
probe is confident.

## 6. Correcting the record

The comparison in `graphrag-performance-metrics.md` shows ADAPTIVE losing to FIXED on
MRR by 6.8% (2Wiki) and 13.0% (HotpotQA). Section 3 explains that gap: the graph
weighting. The two documents are consistent - this one identifies the cause, the other
reports the effect.

Two claims from earlier material are not supported by the current measurements and should
be disregarded:

- *"Graph expansion supplies the evidence; text channels carry almost no signal."* Not
  true on these corpora. BM25 recall is 0.80 (2Wiki) and 0.97 (HotpotQA) versus graph
  0.50 and 0.38. The earlier figure of 0.002 was a scoring artefact against a corpus that
  no longer exists - see `graphrag-external-reproduction.md`, section 7.
- *"Adaptive preserves most of graph-only's quality while doing less graph work."* True
  only relative to graph-only. Adaptive beats graph-only, but graph-only is the weakest
  non-degenerate method, and adaptive loses to fixed hybrid.

## 7. Limitations of the current decision module

Stated as open problems. Each is a concrete starting point, not a vague aspiration.

### 7.1 The policy is a static rule table

`DeterministicRulePolicy` cannot learn. Moving to a fitted policy raises two questions
worth investigating: which features predict when graph helps, and whether a learned
policy can stay reproducible enough for the audit trail the design requires.

### 7.2 Thresholds are unvalidated defaults

Seven thresholds, none swept. `high-average-idf=1.5` against an observed median of 3.01
is the clearest example: it was presumably chosen for a different corpus profile. A
one-at-a-time sweep on held-out data would establish which thresholds are load-bearing.

### 7.3 Fusion weights are a global constant

`graph-weight` applies to every query. Section 3 shows a single global value is wrong for
at least these two datasets. Per-query weighting - for example scaling graph weight by
anchor confidence - is unexplored.

### 7.4 Replanning has no measured benefit

It fires and does nothing. Either the replan triggers are wrong, or the budget it
reallocates is not binding. Both hypotheses are testable and neither has been tested.

### 7.5 No online signal

The policy cannot use feedback from retrieved evidence. A deployment that observes which
evidence was cited could adjust weights per query class; nothing in the current design
supports this.

### 7.6 Two of the seven rules are unreachable

`high-average-idf` and `high-candidate-degree` cannot fire at their defaults. Either the
thresholds are miscalibrated or the features are wrong for these corpora. Determining
which requires data this report does not have.

## 8. Recommendations

In priority order, for whoever continues this work.

1. **Re-derive `graph-weight` and `text-weight` from data.** The ablation shows this is
   the single highest-impact change. A sweep over a held-out slice would settle it.
2. **Delete or re-calibrate the two unreachable rules.** They add trace noise and imply a
   behaviour the system does not have.
3. **Either make replanning matter or remove it.** 1.21 no-op revisions per request is
   cost without benefit.
4. **Report the graph prior as a hypothesis, not a result.** The design's premise that
   graph evidence should dominate is not supported on these corpora.
5. **Treat the answer layer as unvalidated.** F1 at 0.0095 with EM at 0 means the
   extractive consumer does not answer these questions. Any claim about end-to-end answer
   quality needs a generative model and a fresh evaluation.

## 9. Provenance

| Item | Path |
| --- | --- |
| Ablation run | `tmp/graphrag/data/runs/ablations-component/2wikimultihopqa-20261112T093325Z` |
| Combined summary | `<run>/component-ablation.csv` |
| Per-condition results | `<run>/<condition>/2wikimultihopqa-<condition>.csv` |
| Runner | `tools/graphrag/ablate_adaptive.sh` |

Dataset: 2WikiMultiHopQA dev, first 200 queries, `chars4-v1` token estimator, seed 0.
Component ablations were not run on HotpotQA; the ranking gap there (13.0% below fixed
hybrid) is larger than on 2Wiki, so the same mechanism is likely present but stronger, and
should be measured rather than assumed.