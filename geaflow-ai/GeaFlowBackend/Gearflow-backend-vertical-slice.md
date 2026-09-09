# Minimal GeaFlow Backend Vertical Slice — Design

Status: **Open / For review** · Scope: geaflow-ai · Issue: apache/geaflow#849

## 1. Context & Goals

`geaflow-ai` today is a separate, in-process RAG / graph-memory library. It builds
a `MemoryGraph` in the JVM and serves retrieval through `GraphMemoryServer`. The main
GeaFlow engine (DSL + runtime) is **not** wired in as a Graph Memory backend —
`GraphComputeEngine` is an empty marker interface, and nothing consumes it.

This document scopes the **minimal vertical slice** that lets a `GeaFlowBackend`
treat the real GeaFlow engine as a backend of geaflow-ai, so the two gain one shared
contract early instead of diverging. It deliberately does **not** implement anything
(see Issue 848 for the local persistent prototype, Issue 847 for the `GraphBackend`
SPI).

> This is a scoping/design deliverable only. No production HA claim is made.

## 2. Current State (two paths, one in-memory store)

| Concern          | Abstraction                                                                                       | Impl today                                 |
| ---------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| Read / retrieval | `GraphAccessor` (`getVertex`, `getEdge`, `scanVertex`, `scanEdge`, `expand`, `copy`)              | `LocalMemoryGraphAccessor` → `MemoryGraph` |
| Write / mutation | `MutableGraph` (`addVertex`, `updateVertex`, `removeVertex`, `addEdge`, `removeEdge`, schema ops) | `MemoryMutableGraph` → `MemoryGraph`       |
| Backend seam     | `GraphComputeEngine`                                                                              | **empty interface, unused**                |
| Server entry     | `GraphMemoryServer`                                                                               | iterates configured index stores           |

Observations:

- Everything funnels to `MemoryGraph` (in-JVM maps), loaded once from graph files.

- Writes are in-memory only; nothing survives restart, and there is no transaction /
  checkpoint boundary.

- The natural seam for a real backend already exists (`GraphComputeEngine`) but is empty.

## 3. Goal of the Slice

Prove that geaflow-ai can address GeaFlow-the-engine through one **shared, stable,
testable interface**, with the smallest surface that still exercises read + write + a
restart/checkpoint story end to end.

## 4. Options for the First Slice

Maintainers must decide among three candidates:

### 4.1 Option A — Read-only slice

- GeaFlow engine ingests the graph (already possible via GQL) and geaflow-ai reads it
  only through `GraphAccessor` backed by a GeaFlow-backed accessor.

- **Pros**: smallest surface; reuses existing read API; fastest to land.

- **Cons**: does not exercise writes, so mutation/checkpoint risks surface later.

- **Test scenario**: ingest a 3-vertex / 2-edge graph via GQL, then run
  `getVertex("person","alice")` and `scanEdge(aliceVertex)`; expect the 2 edges back,
  ordered deterministically.

- **Existing tests that change**: may defer; the GeaFlow-backed accessor would need a
  new parity suite (cf. Issue 850), but no `LocalMemoryGraphAccessor` test set is broken
  because only the fake backend is swapped as a mirror.

- **Effort**: \~3 developer-days (read-path accessor + local-run params).

### 4.2 Option B — Write-through slice  *(recommended)*

- geaflow-ai writes via a GeaFlow-backed `MutableGraph`; GeaFlow materializes the graph
  in its own store; reads come back through the same `GraphAccessor`.

- **Pros**: covers both paths, gives a real write -> restart -> read loop, matches how
  the extracted-facts pipeline (Issues 837–845) actually produces graphs.

- **Cons**: needs write-path mapping (DSL table/state) which touches more runtime surface.

- **Test scenario**: `addVertex(alice)` + `addEdge(alice→bob)` -> `flush/checkpoint` ->
  new JVM -> `getVertex/getEdge` return identical `alice`,`bob`,`alice→bob`; then
  `updateVertex(alice.age=30)` and re-checkpoint reads the updated value (idempotent
  replay).

- **Existing tests that change**: the shared contract suite (cf. Issue 850) now runs
  against **both** the in-memory fake and the GeaFlow-backed backend; mutation tests
  (`addVertexSchema` / `updateVertex` / `removeEdge`) must pass on both.

- **Effort**: \~7–10 developer-days (write-path mapping is the bulk).

### 4.3 Option C — Projector-based slice

- A GeaFlow job projects/subgraphs the graph, and geaflow-ai consumes the projection.

- **Pros**: cleanly uses GeaFlow's vertex-centric model.

- **Cons**: higher conceptual overhead; more moving parts for a first slice.

- **Test scenario**: run a projection job that emits the 2-hop neighborhood of `alice`;
  expect a subgraph of exactly {alice, bob, carol} and their edges, before any query.

- **Existing tests that change**: introduces a new job-orchestration surface (the
  projector job) rather than touching the accessor; the geaflow-ai suite gains a
  subgraph-contract test but no existing suite is rewritten.

- **Effort**: \~10–12 developer-days (highest; projector job + handoff).

### 4.4 Decision matrix

| Criterion                   | A (read-only) | B (write-through) | C (projector)   |
| --------------------------- | ------------- | ----------------- | --------------- |
| Exercises write path        | no            | yes               | n/a (job side)  |
| write → restart → read loop | no            | **yes**           | indirect        |
| Runtime surface touched     | minimal       | moderate          | largest         |
| Effort                      | \~3 d         | \~7–10 d          | \~10–12 d       |
| Fallback safety             | —             | degrades to A     | degrades to B/A |

> Means of comparison only; effort is a relative sizing for maintainers, not a commitment.

## 5. Supported Operations (assuming Option B as the baseline)

- **Write**: `addVertex/updateVertex/removeVertex`, `addEdge/removeEdge`,
  `addVertexSchema/addEdgeSchema` (the existing `MutableGraph` method set).

- **Read**: `getVertex`, `getEdge`, `scanVertex`, `scanEdge`, `expand` (existing
  `GraphAccessor` method set).

- **Lifecycle**: open/close backend; flush; a defined checkpoint/commit point; scan
  normalization (sorted/deterministic where contract requires).

## 6. Non-goals (explicitly out of scope for the slice)

- No production HA / distributed consistency guarantees.

- No ANN / vector-search integration work (separate line of issues).

- No replacement of the existing `MemoryGraph` main path everywhere.

- No schema evolution or cross-tenant semantics in this slice.

- No bit-identical parity with HugeGraph Server.

## 7. Mapping to Existing GeaFlow DSL/Runtime Components

| GeaFlow component                                           | Role in the slice                                                                                                         |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| GQL / `geaflow-dsl`                                         | Authoritative graph schema + ingest; express read projections                                                             |
| Vertex-centric API (`AlgorithmUserFunction`, `sendMessage`) | `scanVertex`/`scanEdge`/`expand` semantics via iteration/messaging (cf. `udf/graph/PageRank`, `SingleSourceShortestPath`) |
| State store (`geaflow-store`, rocksdb/memory)               | Where upserted vertices/edges physically live; basis of checkpoint                                                        |
| File IO / persistent root (`geaflow.file.persistent.root`)  | Export/import of graph snapshots for restart+reload tests                                                                 |
| Local run (`LocalClusterManager` / GQL client)              | Cheap way to run the slice in integration tests                                                                           |

## 8. Checkpoint Behavior

- Define a **commit/flush boundary** that persists outstanding writes to the GeaFlow
  store atomically (reuse engine checkpoint if available; otherwise a per-backend
  flush marker).

- On restart, reload from the last completed checkpoint; any write after the last
  checkpoint must be either re-applied idempotently or fail closed — never silently lost.

- Expose the checkpoint/lag state so a later `SearchableWatermark` (Issue 857) can read it.

## 9. Missing APIs / Blockers to Resolve

The slice must close these gaps before/when it lands. Where a blocker is "empty yet",
a minimum design is proposed here — as a decision, not a code sketch.

1. **`GraphComputeEngine`** **lifecycle contract.** The interface is empty; that is the
   *problem*, not the answer. The slice should agree a minimal lifecycle up-front:

   - `init(config)` — parse backend config (root dir, store type), open resources;

   - `close()` — release resources, no-op if never `init`ed;

   - capability flags exposed via a single read-only query (e.g. `capabilities()`
     returning whether the backend is writable / restartable / in-process), so a
     caller never assumes behavior.
     This is the smallest contract that lets the runtime open/close a real engine
     without leaking implementation details.

2. **Cross-process read semantics for** **`scanVertex`/`scanEdge`.** Both return a Java
   `Iterator` (`GraphAccessor`) — an in-process object graph that cannot cross a
   process/job boundary. A GeaFlow-backed accessor must choose a strategy:

   - **Lazy fetch**: iterate a server-side cursor and page results (streaming, needs
     a companion cursor API); or

   - **Materialized list**: run the iteration eagerly and return a bounded snapshot
     (simple, memory-bounded, fits small test graphs); or

   - **Native traversal bypass**: express scan/expand natively in GeaFlow's
     vertex-centric iteration and return a result set rather than an `Iterator`.
     Recommendation for the slice: **materialized list**, because the vertical-slice
     graphs are small and it keeps `scanEdge(GraphVertex)`'s signature stable as a
     drop-in for the fake backend. Documenting this choice now avoids a silent
     cross-process break later.

3. **Write-path mapping.** No path exists from `MutableGraph` to a GeaFlow DSL/state
   surface. Pick **table-based** for the slice (GQL-computable, introspection-friendly)
   over raw state-store writes (faster but bypasses schema). This is Option B's bulk.

4. **Identity semantics.** `scanEdge(GraphVertex)` and `MemoryGraph` key on in-memory
   objects; a GeaFlow-backed impl must define equal `id`/`label` semantics
   (`getVertex(label,id)` as the canonical key) so the two backends stay
   interchangeable in the same contract suite.

5. **Backend wiring / registration.** `GraphMemoryServer` does **not** discover
   backends through `GraphComputeEngine` — it collects `GraphAccessor` via
   `addGraphAccessor(...)` and `IndexStore` via `addIndexStore(...)`, and `search()`
   only ever reads `graphAccessors.get(0)`. So the integration seam to settle is:
   how does a newly built GeaFlow-backed accessor get **registered** so the server
   picks it up — a build-time binding, a config key that selects the backend type, or
   promotion of a `GraphBackend` SPI (Issue 847)? This doc should record the chosen
   registration point so the wiring is explicit, not implicit in a test harness.

6. **Startup trace signal.** No standard "backend started / graph loaded" signal for
   retrieval-parity checks; define one log/metric line the parity suite can assert on.

## 10. Test Strategy

- **Contract tests** shared by a fake in-memory backend and the GeaFlow-backed backend
  (same `GraphBackend`-level suite; cf. Issue 850).

- **Round-trip**: write → flush/checkpoint → new JVM instance → read back equal graph.

- **Failure injection**: truncated/partial write fails closed or is quarantined
  (aligns with Issue 846/848).

- **Idempotent replay**: re-applying the same upserts yields one logical state
  (aligns with Issue 845).

- Run the suite locally via the existing local-run path (fast, no external services).

## 11. Rollback & Compatibility

- Keep the existing `MemoryGraph` path untouched and default; the GeaFlow backend is an
  opt-in implementation of the shared contract.

- No breaking change to `GraphAccessor`/`MutableGraph` signatures in this slice; new
  contracts are additive.

- Public SPI additions require maintainer review before promotion.

### Migration paths between approaches

- **B → A (write path too risky)**: stop routing `MutableGraph` writes to GeaFlow; the
  accessor keeps serving reads. No data migration needed — the GeaFlow store is
  already materialized from the ingested graph, so reads stay valid.

- **B → C (projection becomes the real need)**: add the projector job as a read-side
  producer; writes remain as-is. A/C can coexist because both read from the same store.

- **C → B (need write-back)**: the projector stays but geaflow-ai writes land on the
  store directly, with the projector re-run to refresh projections — a refresh, not a
  rewrite of the writer.

- **Any → A**: A is the common floor; every option degrades to read-only without
  touching the default `MemoryGraph` path.

## 12. Open Questions

- Write target: GeaFlow **DSL table** vs directly against **state store**? (affects §7 map)

- Does a local GeaFlow instance run embedded in-process, or as a separate local job
  the slice shells out to?

- Who owns the `GraphComputeEngine` lifecycle contract — this slice or Issue 847 SPI?

## 13. Recommendation

Proceed with **Option B (write-through)** as the primary vertical slice, with **Option A
as a committed fallback**. After the B read/write loop proves in the local run, escalate
to the shared contract suite (Issue 850) before any broader rollout.

**Rollback trigger & plan**: if the write-path mapping (Blockers 3/5) proves unstable
or over-scoped, revert to **A (read-only)** for the first landed slice and re-open B via
the §11 `B → A` migration path (reads stay valid, no data move). The decision record is
the record of why B was chosen and under what condition it degrades — so a later
maintainer can reverse it without re-deriving the context.

**Decision inputs land first**: resolve §12 open questions (write target; embedded vs
separate local job; `GraphComputeEngine` lifecycle ownership) **in this design phase**,
because each one changes the blocker proposal above. A maintainer should be able to say
"yes, go with B" from this document alone, without inspecting Java code — that is the
acceptance bar for this design doc.

Land this design first, then implement against 847 (`GraphBackend`) reusing these
decisions.
