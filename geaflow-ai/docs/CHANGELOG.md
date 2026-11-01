# GraphRAG changelog

All changes to the `geaflow-ai` GraphRAG retrieval engine. This file records the
delivered scope of the F-01..F-10 work items plus the follow-up gap fixes.

## Unreleased

### Added

- `Fusion` SPI for pluggable score fusion. `WeightedRrfFusion` remains the
  default; the active strategy is recorded as `trace.fusionVersion`.
- `RecallStopReason.LOW_ASSOCIATION_SCORE`, `GraphStats.getFilteredByAssociation()`
  and `GraphStats.getTerminatedHop()` so a hop abandoned by the association floor
  is observable in the trace.
- `GraphRetrievalConfig.minAssociationSurvivalRate` (default `1.0`, i.e. disabled)
  controlling how many scored edges must pass the association floor before a hop
  keeps consuming edge-scan budget.
- `AnswerResponse.getCitationCoverage()` and
  `AnswerResponse.getConsideredEvidenceCount()` reporting what fraction of the
  offered evidence actually reached the citation list.
- `benchmark_http.py --ablation` for the five required conditions
  (`no-anchor`, `high-degree`, `lexical`, `semantic`, `multi-hop`), applied to
  `FIXED_HYBRID` and `ADAPTIVE_HYBRID`; results carry an `ablation` column.
- `geaflow-ai/docs/graphrag-operations.md` now documents all five retrieval
  modes, three execution modes, all eight typed error codes, the full
  configuration table with hard ceilings, four request examples
  (success / no-anchor / partial / typed error), dataset licences, cache
  requirements, a troubleshooting table and a version matrix.

### Fixed

- A caller-supplied `Fusion` strategy was silently replaced by the default
  weighted RRF both at the routing-plan branch and when `retrieve` delegated
  through the fixture registry lease.
- `/ready` built its JSON by string concatenation, so a configured graph name or
  version containing a quote, backslash or control character produced an invalid
  document. It is now escaped, and a missing value is emitted as JSON `null`
  instead of the literal `null` string.
- HTTP contract coverage for five modes and three execution modes; `KEYWORD` was
  previously untested at the HTTP layer.

### Changed

- `F-04` association floor now stops the whole hop instead of only skipping the
  individual edge. The behaviour is opt-in: the default survival rate of `1.0`
  reproduces the previous edge-by-edge filtering exactly.

## Delivered work items

| Item | Scope |
|---|---|
| F-01 | Unified evidence model, stable IDs, provenance contract, JSON round-trip |
| F-02 | Dataset manifests, artifact build/publish, import state machine, quality counters |
| F-03 | Versioned graph view: snapshot, delta, tombstones, lease stability |
| F-04 | Bounded 1-2 hop graph expansion, deterministic sampling, diffusion stops |
| F-05 | Sequential/parallel/cascaded execution, unified budget ledger, reranker SPI |
| F-06 | Deterministic adaptive routing, plan validation with recorded adjustments, bounded replanning |
| F-07 | Solon REST service, readiness, metrics, typed errors, request-id propagation |
| F-08 | HotpotQA and 2WikiMultiHopQA adapters, six-method comparison, ablation |
| F-09 | Extractive answer consumer, citations, structured abstention |
| F-10 | Operations guide, release checklist, version matrix, this changelog |