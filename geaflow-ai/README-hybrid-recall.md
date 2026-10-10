<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Fixed Hybrid Recall MVP

`RecallService` is a session-free Java facade over offline, versioned fixtures. Its structured request
supports `BM25_ONLY`, `VECTOR_ONLY`, `GRAPH_ONLY`, and `HYBRID`; `KEYWORD` aliases `BM25_ONLY` in this core
facade. The existing HTTP `/api/v1/retrievals` endpoint supports only `KEYWORD`. Other modes return
`UNSUPPORTED_OPTION` there; HTTP Hybrid serving is not part of this implementation.

## Load and retrieve

Build both artifacts with `LuceneBm25IndexBuilder` and `OfflineVectorIndexBuilder`, using the same
`IngestionContext` and canonical chunks. Supply precomputed vectors; no embedding service is called.
Load the published artifacts and graph records once:

Each canonical chunk must carry its real `sourceUri` (or use `TextChunk.withSourceUri` before building).
The ingestion pipeline copies this URI from its `SourceDocument` and rejects missing or inconsistent
provenance. The registry rejects chunks without a URI; citations use this URI and the chunk offsets.

```java
RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
registry.load(graphName, graphVersion, indexVersion, bm25Path, vectorPath,
    chunks, entities, edges, vectorSource, vectorVersion);
RecallService service = new RecallService(registry, new WeightedRrfConfig(1.0, 1.0, 1.0, 60));

RetrievalRequest request = new RetrievalRequest();
request.setGraphName(graphName);
request.setGraphVersion(graphVersion);
request.setIndexVersion(indexVersion);
request.setMode("HYBRID");
request.setQuery("Confucius");
request.setQueryVector(Arrays.asList(1.0, 0.0)); // Must match the artifact's dimensions.
request.setBudget(new RetrievalBudget(10, 3000, 100, 4096));
request.setAllowPartialResults(true);
RetrievalResponse result = service.retrieve(request);
```

`graphVersion` and `indexVersion` are required and must match. The registry validates the persisted
artifact graph name, graph/index versions, and a fingerprint of the complete canonical chunk payload.
Optional request vector source/version constraints must match the loaded vector artifact. Keep the
readers alive while their fixtures are in use; callers own the lifecycle of registered readers.
The legacy overload accepting text, a float vector, and an absolute deadline requires a service
constructed directly with readers, rather than a registry-backed service.

## Budgets, ranking, and limitations

Hybrid runs BM25, Vector, and one-hop Graph under one monotonic deadline starting at request entry. It
allocates a deterministic three-way budget: BM25 receives the base plus the first remainder, Vector
receives the base plus the second remainder, and Graph receives the base, where `base = floor(maxCandidates / 3)`.
Hybrid requires `maxCandidates >= 3`; all modes require `1 <= topK <= maxCandidates`.
`topK` is applied only after merging and ranking. Individual retriever convenience methods still
return at most their requested `topK`; the facade uses the full bounded candidate lists for fusion.

`maxCandidates` caps actual scoring work: matched Lucene documents scored for BM25, vectors evaluated
for Vector, and edges examined for GRAPH_ONLY. Lucene uses a collector that checks the deadline before
scoring each matched document. Parsing, query setup, and sorting are checked at stage boundaries;
deadlines are cooperative, rather than a thread-interruption guarantee.

Both text and vector channels are bounded approximate recall when their evaluation budget is smaller
than the searchable data. BM25 collects Lucene traversal order; Vector scans stable chunk-ID order and
sorts only the evaluated prefix by cosine similarity. The vector result is **not** a global nearest
neighbor guarantee. Increasing the budget widens the evaluated set. ANN search and adaptive candidate
selection remain future work.

Weighted RRF uses `sum(weight[channel] / (rankConstant + rank[channel]))`, with defaults `1.0`, `1.0`,
`1.0`, and `60`. Weights must be finite and non-negative, at least one positive; the rank constant is positive.
Zero-weight channels still run and retain their scores/provenance. Evidence merges by canonical
document/chunk identity, uses stable evidence ID for ties, and receives consecutive final ranks.

GRAPH_ONLY retains deterministic anchor resolution and one-hop expansion. Both edge scans and unique
chunk evidence are capped by `maxCandidates`, including edges that cite several chunks. Paths and
anchor provenance survive merging. Hybrid always selects BM25, Vector, and Graph; graph provenance
contains the request ID, graph version, anchor match, path, and supporting chunk IDs. A supplied request
ID is resolved before any channel runs; otherwise one is generated. Every graph evidence `queryId`
matches the response and retrieval trace request ID. The graph evidence trace does not store query text.

## Errors and trace

Missing, empty, non-finite, float-overflowing, zero-norm, or dimension-mismatched query vectors return
`INVALID_REQUEST` for vector modes. Unknown modes and non-sequential execution return
`UNSUPPORTED_OPTION`. Malformed JSON is rejected at the codec boundary.

The loader records channel-specific missing/corrupt artifact failures as `INDEX_NOT_READY` and only
publishes a READY fixture when at least one channel is usable. With partial results disabled, a failed
selected channel fails the request. With partial results enabled, its evidence is omitted and the
successful channel's evidence remains. If the request deadline expires, strict requests return
`RETRIEVAL_TIMEOUT`; partial requests retain fully evaluated evidence and mark unfinished channels.

Trace fields include versions, selected channels, effective topK, per-channel budgets, actual evaluated
counts, candidate counts before fusion/topK, graph anchors/edges/neighbors/vertices, validation errors,
elapsed nanoseconds, and the effective three-channel RRF configuration.
`channelStatuses` distinguishes `SUCCESS`, `DEGRADED`, and `NOT_RUN`; `channelStopReasons` carries typed
`COMPLETED`, `CANDIDATE_BUDGET`, `CANDIDATE_LIMIT`, `EDGE_SCAN_LIMIT`, `NO_RELIABLE_ANCHOR`, `DEADLINE`,
or `INDEX_NOT_READY`. `degradationReasons` carries explanatory text separately. Overall `stopReason`
prioritizes deadline, graph trace validation, graph edge/candidate limits, candidate budget, then final topK
truncation, then no results or normal completion. Candidate-budget exhaustion is a valid bounded stage,
not an index failure. A fusion-stage deadline is also reflected in its stage status.

## Artifact compatibility and verification

BM25 stores identity in Lucene commit metadata. Vector artifacts use `GEAFLOW-VECTOR-2`, adding identity
before vector source/version and the vector payload. Legacy artifacts without this metadata are
rejected with `INDEX_NOT_READY`; rebuild and publish a fresh graph/index version before switching
requests. Existing immutable artifacts are validated rather than silently overwritten.

Chunk fingerprints now include `sourceUri`; artifacts built with the previous fingerprint must be
rebuilt. All three Java artifact builders use a shared publisher with bounded JVM locks and an OS file
lock per canonical target. Publication checks and rename run under that lock; competing builders reuse
an equivalent winner and reject conflicting content. The `.publish.lock` file stays beside the artifact
so every process locks the same inode. Do not delete it while builders are running.

`MetadataPublisher.begin` returns an `ImportAttempt` ownership token. `publish` and `fail` require that
token. A failed `begin` does not trigger `fail`, and a competing ingestion cannot fail the owner.
Custom publishers must implement the token-aware lifecycle.

## HTTP access and keyword index lifecycle

The server binds to `127.0.0.1` by default. Non-loopback binding requires both
`retrieval.remote-access-enabled=true` and a nonblank `retrieval.api-token`; invalid combinations fail
configuration initialization. Environment equivalents are `GEAFLOW_SERVER_HOST`,
`GEAFLOW_REMOTE_ACCESS_ENABLED`, and `GEAFLOW_RETRIEVAL_API_TOKEN`. Remote mode requires
`Authorization: Bearer <token>` on retrieval and the legacy `/graph/*` and `/query/*` data APIs.
Missing credentials return `401`; invalid credentials return `403`. `/health` remains available.
The single token grants access to the configured server; per-graph authorization is future work.

Responses use the same configured limits as request validation, including a raised `max-top-k`.
Keyword retrieval builds one complete Lucene store on the first request and reuses it. Candidate
budgets cap search results evaluated rather than the vertices included in the cached index. Graph
updates invalidate the cache; server replacement and shutdown close it. Cold construction and lock
acquisition check the request deadline. Search store closure is idempotent.

## Python ingestion

Multi-file ZIP sources select the requested train/dev/test entry by case-insensitive basename;
missing or ambiguous splits fail. Single archives with a generic data filename remain supported.
Artifact identity includes dataset release and the effective chunk policy (size, overlap, and token
estimate settings). The policy fingerprint also participates in chunk IDs, even for short documents.

Ingestion normalizes one batch at a time, writes canonical `documents.jsonl` and `chunks.jsonl`, and
merges graph records and provenance in a temporary SQLite database. Index builder callbacks receive a
repeatable `JsonlRecords` iterable supporting `len`; callbacks must iterate rather than index a list.
Graph export reads sorted records from disk. Temporary database and staging files are removed on
success or failure; a failed attempt retains only a separate `*.FAILED.json` diagnostic. Canonical
JSONL files remain in the published version for reproducibility. Memory depends on batch size, an
individual document's chunks, and a single exported graph record's provenance, rather than simultaneous
materialization of the entire corpus. This does not bound allocations made by external builder callbacks.

Run the retrieval and HTTP regression suite with JDK 11, which is compatible with the repository's
JaCoCo 0.8.8:

```bash
mvn -pl geaflow-ai -am clean test -Drat.skip=true \
  -Dtest='org.apache.geaflow.ai.retrieval.**.*Test,MemoryServerTest,GeaFlowMemoryServerTest,GraphMemoryKeywordSearchTest,SearchStoreTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
python -m pytest tools/graphrag/tests -q
```

The Maven lifecycle also runs Checkstyle and Apache RAT. New retrieval production sources have no
blanket Checkstyle suppressions. The suite covers bounded scoring, partial-channel failures,
deadlines using a controlled clock, graph limits, version mismatches, JSON trace round trips,
concurrent request isolation, and rejection of core modes at the HTTP boundary.
The PR #876 regressions also cover concurrent import ownership, same-target publication for all three
artifacts, competing JVMs, URI citations, request IDs, HTTP authentication and custom limits, cached
keyword retrieval, repeated close, ZIP splits, release/policy identities, and a 10,000-document spill
fixture whose traced Python heap stays below 12 MiB. This fixture uses deterministic builder callbacks;
it is not a full HotpotQA/2Wiki or external embedding benchmark.
