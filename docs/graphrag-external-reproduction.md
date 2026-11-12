# GraphRAG Retrieval: External Reproduction Guide

This document lets an independent party reproduce every number in
`graphrag-performance-metrics.md` and `graphrag-adaptive-ablation.md` from a clean
checkout. It states where the data comes from, how to fetch it, what to run, and what
results to expect.

Nothing in this repository contains the benchmark corpora. The datasets are large and
are governed by their own licences, so they are referenced by upstream URL, version and
checksum instead of being vendored.

## 1. What is reproduced

| Artifact | Produced by | In repository |
| --- | --- | --- |
| Retrieval engine and unit tests | Maven | Yes |
| Ingestion, benchmark and load tooling | `tools/graphrag/` | Yes |
| Benchmark datasets (2WikiMultiHopQA, HotpotQA) | upstream download | **No** - fetched, not shipped |

## 2. Data sources

### 2WikiMultiHopQA

| Field | Value |
| --- | --- |
| Upstream | `https://www.dropbox.com/s/npidmtadreo6df2/data.zip?dl=1` |
| Distribution | `2WikiMultiHopQA` dev split, as mirrored by the dataset authors |
| Local cache file | `tmp/graphrag/data/cache/2wikimultihopqa-dev.zip` |
| Size | 246,135,840 bytes |
| SHA-256 | `e8e57c0aafc4a26d41131e320ebb5afb6f2aca86b8a6e6611b08f52033cb7d04` |
| Split used | `dev`, first 500 queries (deterministic prefix, `--limit 500`) |
| Licence | Dataset authors' terms; declared `pending-review` in the manifest |

Verify the download before use:

```bash
sha256sum tmp/graphrag/data/cache/2wikimultihopqa-dev.zip
# expected: e8e57c0aafc4a26d41131e320ebb5afb6f2aca86b8a6e6611b08f52033cb7d04
```

### HotpotQA (distractor)

| Field | Value |
| --- | --- |
| Upstream | `https://hotpotqa.github.io/` - distractor setting |
| Local cache file | `tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json` |
| Size | 45,938,749 bytes |
| SHA-256 | `810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7` |
| Split used | distractor `validation`, first 500 queries |
| Licence | CC BY-SA 4.0 (upstream); declared `pending-review` in the manifest |

```bash
sha256sum tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json
# expected: 810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7
```

If the checksums do not match, stop. A different corpus produces different numbers and
the comparison below no longer applies.

## 3. Environment

| Component | Version used | Notes |
| --- | --- | --- |
| JDK | 21.0.12 (17+ works) | source/target 8 and 11 are also built by CI |
| Maven | 3.8.8 | |
| Python | 3.11 | `tools/graphrag/` is stdlib-only apart from the ingest extras |
| OS | Linux x86_64 | |
| Network | not needed after data download | embeddings are precomputed |

Build the module once:

```bash
mvn -pl geaflow-ai -o -am -DskipTests -Drat.skip=true -Djacoco.skip=true package
```

Run the test suites:

```bash
mvn -pl geaflow-ai -o test -Drat.skip=true -Djacoco.skip=true   # 455 tests
python3 -m pytest tools/graphrag/tests/ -q                       # 114 tests
```

## 4. Reproducing the reported numbers

The pipeline is four stages: ingest the raw corpus into a canonical bundle, publish the
bundle atomically (`READY.json`), start the retrieval service, and run the six-method
benchmark against it. `reproduce.sh` drives all four.

### 4.1 One command per dataset

```bash
# 2WikiMultiHopQA
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa --skip-build --limit 500

# HotpotQA
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/hotpotqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/hotpotqa-bundle-gte
tools/graphrag/reproduce.sh --dataset hotpotqa --skip-build --limit 500
```

`--skip-build` reuses an existing bundle. On a machine without one, drop the flag and
`reproduce.sh` ingests, embeds and publishes first; expect roughly an hour for
2WikiMultiHopQA.

Each run prints its run directory. The summary is
`<runDirectory>/<dataset>-six-methods.csv`.

Runtime on the reference machine: about 40 minutes per dataset for 500 queries x 6
methods (3000 requests).

### 4.2 Why `--answers-url` matters

An earlier report showed `answerF1` as 0 for every method. That was not a measurement:
`/api/v1/retrievals` never returns an answer field, so F1 is only produced when the
separate answer endpoint is also consulted. `reproduce.sh` wires it automatically when
it starts the service itself and leaves the column empty otherwise, rather than
reporting an invented value.

If you run `benchmark_http.py` directly, pass both:

```bash
python3 tools/graphrag/benchmark_http.py \
  --url http://127.0.0.1:18080/api/v1/retrievals \
  --answers-url http://127.0.0.1:18080/api/v1/answers \
  ...
```

### 4.3 Component-level ablation

```bash
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl
tools/graphrag/ablate_adaptive.sh --dataset 2wikimultihopqa --limit 200
```

Eight conditions, each restarting the service with exactly one routing setting
overridden. Output: `<out>/component-ablation.csv` plus per-condition directories.
Runtime: about 50 minutes for 200 queries.

## 5. Expected results

### 2WikiMultiHopQA, 500 queries

| Method | Recall@10 | MRR | NDCG@10 | F1 | EM | edges | tokens | P95 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.7367 | 0.6978 | 0.0022 | 0.0000 | 0.0 | 2806 | 2786 |
| VECTOR_ONLY | 0.7141 | 0.7153 | 0.6406 | 0.0023 | 0.0000 | 0.0 | 2603 | 1132 |
| VECTOR_OVERFETCH | 0.7141 | 0.7153 | 0.6406 | 0.0023 | 0.0000 | 0.0 | 2603 | 945 |
| GRAPH_ONLY | 0.5030 | 0.5259 | 0.4506 | 0.0095 | 0.0000 | 45.9 | 842 | 1248 |
| FIXED_HYBRID | 0.8435 | 0.7703 | 0.7388 | 0.0024 | 0.0000 | 36.3 | 2677 | 2867 |
| ADAPTIVE_HYBRID | 0.8358 | 0.7181 | 0.6982 | 0.0024 | 0.0000 | 30.1 | 2682 | 2905 |

### HotpotQA distractor, 500 queries

| Method | Recall@10 | MRR | NDCG@10 | F1 | EM | edges | tokens | P95 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.8253 | 0.8234 | 0.0024 | 0.0000 | 0.0 | 2854 | 1806 |
| VECTOR_ONLY | 0.8196 | 0.7658 | 0.7120 | 0.0023 | 0.0000 | 0.0 | 2779 | 667 |
| VECTOR_OVERFETCH | 0.8196 | 0.7658 | 0.7120 | 0.0023 | 0.0000 | 0.0 | 2779 | 548 |
| GRAPH_ONLY | 0.3818 | 0.3272 | 0.3055 | 0.0079 | 0.0000 | 20.4 | 658 | 499 |
| FIXED_HYBRID | 0.9460 | 0.8008 | 0.7913 | 0.0024 | 0.0000 | 38.4 | 2828 | 1887 |
| ADAPTIVE_HYBRID | 0.9525 | 0.6970 | 0.7322 | 0.0024 | 0.0000 | 33.7 | 2837 | 1909 |

Tolerance: retrieval is deterministic, so Recall/MRR/NDCG reproduce exactly. Latency
varies with hardware; treat P95 as order-of-magnitude only.

## 6. How to confirm you reproduced correctly

Check these in order. Each one isolates a different failure mode.

| Check | Expected | If it fails |
| --- | --- | --- |
| Bundle published | `READY.json` present, `status` = READY | re-run without `--skip-build` |
| Gold loaded | `unresolvedSupportingFacts` is a small number (28 for 2Wiki, 0 for HotpotQA) | `--gold` did not resolve; check `GEAFLOW_GOLD` |
| Query vectors loaded | `VECTOR_ONLY` recall well above 0.001 | `--vectors` missing; vector methods silently degrade |
| Answer endpoint wired | `answerF1` is non-zero for at least one method | `--answers-url` missing |
| Routing active | `policyId` = `deterministic-rule`, `meanReplanCount` > 0 for ADAPTIVE | service started without `retrieval.*` config |
| Determinism | two runs produce identical Recall/MRR | corpus or bundle changed between runs |

The most common false negative is a run where every quality column is empty. That means
`--gold` did not resolve, not that the engine failed.

## 7. Relationship to the earlier report

An earlier version of the results reported BM25 and VECTOR recall near 0.001 and
graph-only leading every metric. Those runs were produced against a different chunking
of the corpus, and their evidence identifiers do not exist in the bundle this document
describes:

| | Earlier run (`20261104`) | This document (`20261112`) |
| --- | --- | --- |
| First evidence id | `0009eeb41e0f...` | `2db9ae3ba834...` |
| BM25 Recall@10 | 0.0022 | 0.7978 |
| GRAPH_ONLY Recall@10 | 0.5028 | 0.5030 |
| GRAPH_ONLY vs BM25 | graph leads | text leads |

The graph-only number was stable across both, which is why the change went unnoticed.
Everything else moved because the text channels were being scored against chunk ids
from a corpus that no longer existed. **The numbers in this document supersede the
earlier report.** Anyone citing the old figures should re-run using the checksums above.

## 8. Using an LLM to assist reproduction

The retrieval path itself makes no LLM calls: the answer layer is extractive, so F1 is
computed from spans selected out of retrieved chunks. An LLM can still help, but only as
an operator, not as part of the measurement.

Useful prompts, with the caveat that every claim must be checked against a CSV column:

> I have a run directory from `reproduce.sh`. Read `<dataset>-six-methods.csv` and tell
> me which columns are empty and which are populated. Do not infer missing values.

> Compare the `evidenceRecallAtK` of ADAPTIVE_HYBRID and FIXED_HYBRID. Report the
> absolute difference and the relative percentage. Quote both raw values.

> The `answerF1` column is 0.002 for every method while `unresolvedSupportingFacts` is 0.
> Which wiring step should I check first, and which file decides it?

What an LLM must **not** be used for: filling in missing metrics, estimating what a
number "should" be, or summarising an ablation it has not been given the CSV for. Every
figure in the companion reports traces to a row in a CSV under
`tmp/graphrag/data/runs/`.

## 9. Reporting a reproduction failure

Include: the run directory name, the two dataset checksums, the failing check from
section 6, and the first 20 lines of `service.log`. Without the checksums it is not
possible to tell a code difference from a corpus difference.