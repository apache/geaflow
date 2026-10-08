/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GraphRagMvpEndToEndTest {
    private static final String GRAPH = "mvp-graph";
    private static final String VERSION = "v1";

    @TempDir
    Path tempDir;
    private final List<Bm25IndexReader> readers = new ArrayList<>();

    @AfterEach
    void closeReaders() throws Exception {
        for (Bm25IndexReader reader : readers) {
            reader.close();
        }
    }

    @Test
    void executesAllMvpModesAndPreservesEvidence() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);

        RetrievalResponse keyword = service.retrieve(request("KEYWORD", null));
        assertEquals("BM25_ONLY", keyword.getTrace().getSelectedMode().name());
        assertFalse(keyword.getEvidence().isEmpty());

        RetrievalResponse bm25 = service.retrieve(request("BM25_ONLY", null));
        RetrievalResponse vector = service.retrieve(request("VECTOR_ONLY", Arrays.asList(1.0, 0.0)));
        RetrievalResponse graph = service.retrieve(request("GRAPH_ONLY", null));
        RetrievalResponse hybrid = service.retrieve(request("HYBRID", Arrays.asList(1.0, 0.0)));

        assertFalse(bm25.getEvidence().isEmpty());
        assertFalse(vector.getEvidence().isEmpty());
        assertFalse(graph.getEvidence().isEmpty());
        RetrievalRequest aliasRequest = request("GRAPH_ONLY", null);
        aliasRequest.setQuery("kongzi");
        assertFalse(service.retrieve(aliasRequest).getEvidence().isEmpty());
        assertFalse(hybrid.getEvidence().isEmpty());
        Evidence merged = find(hybrid.getEvidence(), "5:doc-3c3");
        assertTrue(merged.getStageScores().containsKey("BM25"));
        assertTrue(merged.getStageScores().containsKey("VECTOR"));
        assertTrue(merged.getStageScores().containsKey("GRAPH"));
        assertEquals(1.0 / (60 + merged.getStageScores().get("BM25").getRank())
                + 1.0 / (60 + merged.getStageScores().get("VECTOR").getRank())
                + 1.0 / (60 + merged.getStageScores().get("GRAPH").getRank()),
            merged.getFusedScore(), 1.0e-12);
        assertFalse(merged.getPaths().isEmpty());
        assertFalse(hybrid.getPaths().isEmpty());
        assertFalse(hybrid.getSources().isEmpty());
    }

    @Test
    void rejectsVersionsReadinessAndUnsupportedExecution() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        RetrievalRequest wrongVersion = request("BM25_ONLY", null);
        wrongVersion.setIndexVersion("v2");
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> service.retrieve(wrongVersion));

        RetrievalRequest parallel = request("BM25_ONLY", null);
        parallel.setExecutionMode("PARALLEL");
        assertCode(RetrievalErrorCode.UNSUPPORTED_OPTION, () -> service.retrieve(parallel));

        RetrievalRequest badVector = request("VECTOR_ONLY", Collections.singletonList(1.0));
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> service.retrieve(badVector));

        RetrievalFixtureRegistry notReady = new RetrievalFixtureRegistry();
        notReady.register(GRAPH, VERSION, VERSION, fixture.bm25, fixture.vector,
            fixture.chunks, fixture.entities, fixture.edges, false);
        assertCode(RetrievalErrorCode.INDEX_NOT_READY,
            () -> new RecallService(notReady).retrieve(request("BM25_ONLY", null)));
    }

    @Test
    void requiresStructuredVersions() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);

        RetrievalRequest missingGraphVersion = request("BM25_ONLY", null);
        missingGraphVersion.setGraphVersion(null);
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> service.retrieve(missingGraphVersion));

        RetrievalRequest missingIndexVersion = request("BM25_ONLY", null);
        missingIndexVersion.setIndexVersion(null);
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> service.retrieve(missingIndexVersion));

    }

    @Test
    void enforcesCandidateBudgetAndNoAnchorScan() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        RetrievalRequest request = request("HYBRID", Arrays.asList(1.0, 0.0));
        request.setBudget(new RetrievalBudget(1, 1000, 1, 100));
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> service.retrieve(request));
        request.setBudget(new RetrievalBudget(1, 1000, 3, 100));
        RetrievalResponse response = service.retrieve(request);
        assertEquals(1, response.getEvidence().size());
        assertEquals(1, response.getTrace().getCandidateCounts().get("BM25"));
        assertEquals(1, response.getTrace().getCandidateCounts().get("VECTOR"));
        assertEquals(1, response.getTrace().getCandidateCounts().get("GRAPH"));
        assertEquals(3, response.getTrace().getTotalCandidatesEvaluated());
        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"),
            response.getTrace().getSelectedChannels());
        assertEquals(response.getRequestId(), response.getTrace().getRequestId());

        RetrievalRequest noAnchor = request("GRAPH_ONLY", null);
        noAnchor.setQuery("unknown entity");
        RetrievalResponse empty = service.retrieve(noAnchor);
        assertTrue(empty.getEvidence().isEmpty());
        assertEquals(0, empty.getTrace().getAnchorsConsidered());
        assertEquals(0, empty.getTrace().getEdgesExamined());
    }

    @Test
    void scalesHybridRecallAcrossTenThousandChunks() throws Exception {
        List<TextChunk> chunks = new ArrayList<>();
        Map<String, float[]> vectors = new HashMap<>();
        List<String> graphEvidenceChunkIds = new ArrayList<>();
        for (int index = 0; index < 10_000; index++) {
            String chunkId = String.format("scaled-c%05d", index);
            String documentId = String.format("scaled-doc-%05d", index);
            String text = index % 5 == 0
                ? "Confucius discusses ethics in document " + index
                : "Background material about history in document " + index;
            chunks.add(new TextChunk(chunkId, documentId, 0, 0, text.length(), 6, text));
            vectors.put(chunkId, index % 2 == 0 ? new float[] {1.0F, 0.0F} : new float[] {0.0F, 1.0F});
            if (index % 5 == 0) {
                graphEvidenceChunkIds.add(chunkId);
            }
        }
        IngestionContext context = context();
        Path bm25Path = tempDir.resolve("scaled-bm25");
        Path vectorPath = tempDir.resolve("scaled-vector");
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(bm25Path).build(context, chunks);
             org.apache.geaflow.ai.retrieval.index.IndexArtifact ignoredVector =
                 new OfflineVectorIndexBuilder(vectorPath, vectors, "fixture", VERSION)
                     .build(context, chunks)) {
            // Artifacts are published before readers are opened.
        }
        Bm25IndexReader bm25 = new Bm25IndexReader(bm25Path.resolve("bm25-v1"), VERSION, VERSION);
        readers.add(bm25);
        VectorIndexReader vector = new VectorIndexReader(vectorPath.resolve("vector-v1.bin"),
            VERSION, VERSION, "fixture", VERSION);
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("scaled-e1", "Confucius", "person"),
            new EntityRef("scaled-e2", "Ethics", "topic"));
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef(
            "scaled-edge-1", "supports", "scaled-e1", "scaled-e2", graphEvidenceChunkIds));
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.register(GRAPH, VERSION, VERSION, bm25, vector, chunks,
            entities, edges, true);

        RetrievalRequest request = request("HYBRID", Arrays.asList(1.0, 0.0));
        request.setBudget(new RetrievalBudget(10, 5000, 400, 100));
        RecallService service = new RecallService(registry);
        RetrievalResponse first = service.retrieve(request);
        RetrievalResponse second = service.retrieve(request);

        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"), first.getTrace().getSelectedChannels());
        assertEquals(134, first.getTrace().getChannelBudgets().get("BM25"));
        assertEquals(133, first.getTrace().getChannelBudgets().get("VECTOR"));
        assertEquals(133, first.getTrace().getChannelBudgets().get("GRAPH"));
        assertTrue(first.getTrace().getEvaluatedCounts().get("BM25") <= 134);
        assertTrue(first.getTrace().getEvaluatedCounts().get("VECTOR") <= 133);
        assertTrue(first.getTrace().getEvaluatedCounts().get("GRAPH") <= 133);
        assertEquals(133, first.getTrace().getCandidateCounts().get("GRAPH"));
        assertEquals(133, first.getTrace().getGraphCandidatesProduced());
        assertTrue(first.getTrace().getTotalCandidatesEvaluated() <= 400);
        assertTrue(first.getEvidence().size() <= 10);
        assertEquals(first.getEvidence(), second.getEvidence());
        assertEquals(first.getTrace().getCandidateCounts(), second.getTrace().getCandidateCounts());
    }

    @Test
    void producesStableEvidenceAndTraceCounters() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        RetrievalRequest request = request("HYBRID", Arrays.asList(1.0, 0.0));
        RetrievalResponse first = service.retrieve(request);
        RetrievalResponse second = service.retrieve(request);
        assertEquals(first.getEvidence(), second.getEvidence());
        assertEquals(first.getTrace().getCandidateCounts(), second.getTrace().getCandidateCounts());
        assertEquals(first.getTrace().getStopReason(), second.getTrace().getStopReason());
        assertEquals(first.getTrace().getSelectedChannels(), second.getTrace().getSelectedChannels());
        assertEquals(1.0, first.getTrace().getBm25Weight());
        assertEquals(1.0, first.getTrace().getVectorWeight());
        assertEquals(60, first.getTrace().getRrfRankConstant());
    }

    @Test
    void supportsExplicitPartialHybridWhenOneChannelIsUnavailable() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry partialRegistry = new RetrievalFixtureRegistry();
        partialRegistry.register(GRAPH, VERSION, VERSION, null, fixture.vector,
            fixture.chunks, fixture.entities, fixture.edges, true);
        RecallService service = new RecallService(partialRegistry);
        RetrievalRequest request = request("HYBRID", Arrays.asList(1.0, 0.0));

        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> service.retrieve(request));
        request.setAllowPartialResults(true);
        RetrievalResponse response = service.retrieve(request);
        assertFalse(response.getEvidence().isEmpty());
        assertEquals(Arrays.asList("BM25", "GRAPH"), response.getDegradedChannels());
        assertEquals("artifact unavailable",
            response.getTrace().getDegradationReasons().get("BM25"));
    }

    @Test
    void replacementDefersReaderCloseUntilActiveRetrievalLeaseEnds() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        Bm25IndexReader oldReader = new Bm25IndexReader(tempDir.resolve("bm25/bm25-v1"), VERSION, VERSION);
        Bm25IndexReader replacementReader = new Bm25IndexReader(
            tempDir.resolve("bm25/bm25-v1"), VERSION, VERSION);
        registry.register(GRAPH, VERSION, VERSION, oldReader, null,
            fixture.chunks, fixture.entities, fixture.edges, true);

        try (RetrievalFixtureRegistry.Fixture.Lease lease = registry.acquire(GRAPH, VERSION, VERSION)) {
            registry.register(GRAPH, VERSION, VERSION, replacementReader, null,
                fixture.chunks, fixture.entities, fixture.edges, true);
            assertFalse(lease.getFixture().getBm25().search("Confucius", 3, 3).isEmpty());
        } finally {
            registry.close();
        }

        assertThrows(RetrievalException.class, () -> oldReader.search("Confucius", 3, 3));
    }

    @Test
    void clearDefersReaderCloseUntilActiveRetrievalLeaseEnds() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        Bm25IndexReader reader = new Bm25IndexReader(tempDir.resolve("bm25/bm25-v1"), VERSION, VERSION);
        registry.register(GRAPH, VERSION, VERSION, reader, null,
            fixture.chunks, fixture.entities, fixture.edges, true);

        try (RetrievalFixtureRegistry.Fixture.Lease lease = registry.acquire(GRAPH, VERSION, VERSION)) {
            registry.clear();
            assertFalse(lease.getFixture().getBm25().search("Confucius", 3, 3).isEmpty());
        }

        assertThrows(RetrievalException.class, () -> reader.search("Confucius", 3, 3));
        registry.close();
    }

    @Test
    void recordsDeadlineOnTheChannelThatWasStopped() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.bm25, fixture.vector,
            fixture.chunks, fixture.entities, fixture.edges);
        RetrievalResponse response = service.retrieveWithTrace("Confucius",
            new float[] {1.0F, 0.0F}, "HYBRID", 4, 4, System.nanoTime() - 1, true);

        assertEquals("DEADLINE", response.getTrace().getStopReason());
        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"), response.getTrace().getDegradedChannels());
        assertEquals(RecallStopReason.DEADLINE, response.getTrace().getChannelStopReasons().get("BM25"));
        assertEquals(RecallStageStatus.NOT_RUN, response.getTrace().getChannelStatuses().get("VECTOR"));
        assertEquals(0, response.getTrace().getCandidateCounts().get("VECTOR"));
    }

    @Test
    void customRrfConfigurationIsUsedByHybrid() throws Exception {
        Fixture fixture = fixture();
        RetrievalRequest request = request("HYBRID", Arrays.asList(1.0, 0.0));
        request.setQuery("Astronomy");
        RecallService defaultService = new RecallService(fixture.registry);
        RecallService vectorHeavy = new RecallService(fixture.registry,
            new WeightedRrfConfig(0.0, 1.0, 60));

        RetrievalResponse defaultResponse = defaultService.retrieve(request);
        RetrievalResponse weightedResponse = vectorHeavy.retrieve(request);
        assertEquals(1.0, defaultResponse.getTrace().getBm25Weight());
        assertEquals(0.0, weightedResponse.getTrace().getBm25Weight());
        assertEquals(1.0, weightedResponse.getTrace().getVectorWeight());
        assertEquals("5:doc-1c1", weightedResponse.getEvidence().get(0).getEvidenceId());
        assertEquals(1.0 / (60 + weightedResponse.getEvidence().get(0)
                .getStageScores().get("VECTOR").getRank()),
            weightedResponse.getEvidence().get(0).getFusedScore(), 1.0e-12);
        assertTrue(defaultResponse.getEvidence().get(0).getEvidenceId()
            .equals("5:doc-2c2") || defaultResponse.getEvidence().get(0).getEvidenceId()
            .equals("5:doc-3c3"));
    }

    @Test
    void malformedVectorsAreRejectedEvenBeforeExpiredOrMissingChannel() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        for (List<Double> value : Arrays.asList(Collections.<Double>emptyList(), Arrays.asList(0.0, 0.0),
            Arrays.asList(Double.NaN, 1.0), Arrays.asList(Double.MAX_VALUE, 1.0), Collections.singletonList(1.0))) {
            RetrievalRequest invalid = request("HYBRID", value);
            invalid.setAllowPartialResults(true);
            assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> service.retrieve(invalid));
        }
        RecallService direct = new RecallService(fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, fixture.edges);
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> direct.retrieveWithTrace("query",
            new float[] {1.0F}, "HYBRID", 1, 2, System.nanoTime() - 1, true));
        assertCode(RetrievalErrorCode.UNSUPPORTED_OPTION, () -> service.retrieveWithTrace("query",
            null, "ADAPTIVE", 1, 2, 0L));
    }

    @Test
    void strictDeadlineFailsAndPartialDeadlineRetainsCompletedEvidence() throws Exception {
        Fixture fixture = fixture();
        AtomicLong time = new AtomicLong();
        RecallService service = new RecallService(fixture.registry, WeightedRrfConfig.DEFAULT,
            () -> time.getAndAdd(1_000_000L));
        RetrievalRequest timed = request("HYBRID", Arrays.asList(1.0, 0.0));
        timed.setBudget(new RetrievalBudget(2, 7, 4, 100));
        assertCode(RetrievalErrorCode.RETRIEVAL_TIMEOUT, () -> service.retrieve(timed));
        time.set(0L);
        timed.setAllowPartialResults(true);
        RetrievalResponse partial = service.retrieve(timed);
        assertFalse(partial.getEvidence().isEmpty());
        assertEquals("DEADLINE", partial.getTrace().getStopReason());
        assertEquals(RecallStageStatus.DEGRADED, partial.getTrace().getChannelStatuses().get("BM25"));
        assertEquals(RecallStageStatus.NOT_RUN, partial.getTrace().getChannelStatuses().get("VECTOR"));
        assertEquals(0, partial.getTrace().getEvaluatedCounts().get("VECTOR"));
        assertTrue(partial.getTrace().getTotalCandidatesEvaluated() <= 4);
    }

    @Test
    void corruptVectorCanDegradeWithoutLosingBm25Evidence() throws Exception {
        Fixture fixture = fixture();
        Path broken = tempDir.resolve("vector/vector-v1.bin");
        Files.write(broken, new byte[] {0, 1, 2});
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.load(GRAPH, VERSION, VERSION, tempDir.resolve("bm25/bm25-v1"), broken,
            fixture.chunks, fixture.entities, fixture.edges, "fixture", VERSION);
        readers.add(registry.get(GRAPH, VERSION, VERSION).getBm25());
        RecallService service = new RecallService(registry);
        RetrievalRequest hybrid = request("HYBRID", Arrays.asList(1.0, 0.0));
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> service.retrieve(hybrid));
        hybrid.setAllowPartialResults(true);
        RetrievalResponse response = service.retrieve(hybrid);
        assertFalse(response.getEvidence().isEmpty());
        assertEquals(Arrays.asList("VECTOR", "GRAPH"), response.getDegradedChannels());
        assertEquals(RecallStopReason.INDEX_NOT_READY, response.getTrace().getChannelStopReasons().get("VECTOR"));
        assertEquals(RecallStageStatus.DEGRADED, response.getTrace().getChannelStatuses().get("VECTOR"));
        assertTrue(response.getTrace().getDegradationReasons().get("VECTOR").contains("unable to read"));
    }

    @Test
    void loaderPreservesBm25FailureAndRejectsBothFailedChannels() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.load(GRAPH, VERSION, VERSION, tempDir.resolve("absent"), tempDir.resolve("vector/vector-v1.bin"),
            fixture.chunks, fixture.entities, fixture.edges, "fixture", VERSION);
        RetrievalRequest hybrid = request("HYBRID", Arrays.asList(1.0, 0.0));
        hybrid.setAllowPartialResults(true);
        RetrievalResponse response = new RecallService(registry).retrieve(hybrid);
        assertFalse(response.getEvidence().isEmpty());
        assertEquals(Arrays.asList("BM25", "GRAPH"), response.getDegradedChannels());
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> registry.load(GRAPH, VERSION, VERSION,
            tempDir.resolve("absent"), tempDir.resolve("also-absent"), fixture.chunks, fixture.entities,
            fixture.edges, "fixture", VERSION));
    }

    @Test
    void registryRejectsMislabelledReadersAndChangedChunks() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> registry.register(GRAPH, "v2", "v2",
            fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, fixture.edges, true));
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> registry.register("other-graph", VERSION, VERSION,
            fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, fixture.edges, true));
        List<TextChunk> changed = new ArrayList<>(fixture.chunks);
        changed.set(0, new TextChunk("c1", "doc-1", 0, 0, 7, 1, "changed"));
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> registry.register(GRAPH, VERSION, VERSION,
            fixture.bm25, fixture.vector, changed, fixture.entities, fixture.edges, true));
    }

    @Test
    void oddBudgetRanksAndTraceRoundTripAreConsistent() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry, new WeightedRrfConfig(2.0, 0.5, 10));
        RetrievalRequest hybrid = request("HYBRID", Arrays.asList(1.0, 0.0));
        hybrid.setBudget(new RetrievalBudget(2, 1000, 5, 100));
        RetrievalResponse response = service.retrieve(hybrid);
        assertEquals(2, response.getTrace().getChannelBudgets().get("BM25"));
        assertEquals(2, response.getTrace().getChannelBudgets().get("VECTOR"));
        assertEquals(1, response.getTrace().getChannelBudgets().get("GRAPH"));
        assertTrue(response.getTrace().getTotalCandidatesEvaluated() <= 5);
        for (int index = 0; index < response.getEvidence().size(); index++) {
            assertEquals(index + 1, response.getEvidence().get(index).getRank());
        }
        assertTrue(response.getTrace().getStages().get(3).getReason().contains("weighted_rrf_k=10"));
        RetrievalResponse restored = RetrievalApiJson.parseResponse(RetrievalApiJson.toJson(response));
        assertEquals(response.getTrace().getChannelStopReasons(), restored.getTrace().getChannelStopReasons());
        assertEquals(response.getTrace().getChannelStatuses(), restored.getTrace().getChannelStatuses());
        assertEquals(response.getTrace().getEvaluatedCounts(), restored.getTrace().getEvaluatedCounts());
    }

    @Test
    void graphSourcesCannotExceedEvidenceBudgetAndGraphTraceRoundTrips() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef("edge-1", "studies", "e1", "e2",
            Arrays.asList("c1", "c2", "c3")));
        registry.register(GRAPH, VERSION, VERSION, fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, edges, true);
        RetrievalRequest graph = request("GRAPH_ONLY", null);
        graph.setBudget(new RetrievalBudget(1, 1000, 2, 100));
        RetrievalResponse response = new RecallService(registry).retrieve(graph);
        assertEquals(1, response.getEvidence().size());
        assertEquals(2, response.getTrace().getGraphCandidatesProduced());
        assertEquals(1, response.getTrace().getEdgesExamined());
        assertEquals(Collections.singletonList("GRAPH"), response.getTrace().getSelectedChannels());
        assertEquals("CANDIDATE_LIMIT", response.getTrace().getStopReason());
        RetrievalResponse restored = RetrievalApiJson.parseResponse(RetrievalApiJson.toJson(response));
        assertEquals(2, restored.getTrace().getGraphCandidatesProduced());
        assertFalse(restored.getPaths().isEmpty());
    }

    @Test
    void invalidGraphProvenanceIsExcludedAndReportedInTrace() throws Exception {
        Fixture fixture = fixture();
        RetrievalResponse response = new RecallService(fixture.registry).retrieve(request("GRAPH_ONLY", null));

        assertEquals(1, response.getEvidence().size());
        assertEquals("c3", response.getEvidence().get(0).getChunks().get(0).getChunkId());
        assertTrue(response.getTrace().getValidationErrors().contains(
            "edge edge-invalid references an unknown chunk"));
        assertEquals(RecallStopReason.TRACE_VALIDATION_ERROR,
            response.getTrace().getChannelStopReasons().get("GRAPH"));
        assertEquals("TRACE_VALIDATION_ERROR", response.getTrace().getStopReason());
        assertEquals(1, response.getTrace().getGraphCandidatesProduced());
    }

    @Test
    void concurrentRequestsHaveIndependentEvidenceAndTrace() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<RetrievalResponse>> calls = new ArrayList<>();
            for (int index = 0; index < 24; index++) {
                final String mode = index % 2 == 0 ? "HYBRID" : "GRAPH_ONLY";
                calls.add(executor.submit(() -> service.retrieve(request(mode,
                    mode.equals("HYBRID") ? Arrays.asList(1.0, 0.0) : null))));
            }
            for (int index = 0; index < calls.size(); index++) {
                RetrievalResponse response = calls.get(index).get();
                List<String> channels = index % 2 == 0
                    ? Arrays.asList("BM25", "VECTOR", "GRAPH") : Collections.singletonList("GRAPH");
                assertEquals(channels, response.getTrace().getSelectedChannels());
                assertEquals(Collections.singletonList("GRAPH"), response.getDegradedChannels());
                assertFalse(response.getEvidence().isEmpty());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void bm25CountsScoredDocumentsAndStopsInsideCollector() throws Exception {
        Fixture fixture = fixture();
        org.apache.geaflow.ai.retrieval.index.ChannelSearchResult<org.apache.geaflow.ai.retrieval.index.bm25.Bm25Hit> limited =
            fixture.bm25.searchWithStats("Confucius astronomy ethics stars", 1, 1, 0L);
        assertEquals(1, limited.getCandidatesEvaluated());
        assertEquals(1, limited.getHits().size());
        assertEquals(RecallStopReason.CANDIDATE_BUDGET, limited.getStopReason());
        AtomicLong clock = new AtomicLong();
        org.apache.geaflow.ai.retrieval.index.ChannelSearchResult<org.apache.geaflow.ai.retrieval.index.bm25.Bm25Hit> stopped =
            fixture.bm25.searchWithStats("Confucius", 10, 10, 4L, clock::getAndIncrement);
        assertEquals(1, stopped.getCandidatesEvaluated());
        assertEquals(1, stopped.getHits().size());
        assertEquals(RecallStopReason.DEADLINE, stopped.getStopReason());
        assertEquals(0, fixture.bm25.searchWithStats("not-present", 10, 10, 1L, () -> 1L).getCandidatesEvaluated());
    }

    @Test
    void vectorFiniteScanIsExplicitAndDeadlinePreservesEvaluatedHits() throws Exception {
        Fixture fixture = fixture();
        org.apache.geaflow.ai.retrieval.index.ChannelSearchResult<org.apache.geaflow.ai.retrieval.index.vector.VectorHit> prefix =
            fixture.vector.searchWithStats(new float[] {0.0F, 1.0F}, 1, 1, 0L);
        assertEquals(1, prefix.getCandidatesEvaluated());
        assertEquals("c1", prefix.getHits().get(0).getChunkId());
        assertEquals(RecallStopReason.CANDIDATE_BUDGET, prefix.getStopReason());
        assertEquals("c2", fixture.vector.search(new float[] {0.0F, 1.0F}, 3, 1).get(0).getChunkId());
        AtomicLong time = new AtomicLong();
        org.apache.geaflow.ai.retrieval.index.ChannelSearchResult<org.apache.geaflow.ai.retrieval.index.vector.VectorHit> stopped =
            fixture.vector.searchWithStats(new float[] {1.0F, 0.0F}, 3, 3, 2L, time::getAndIncrement);
        assertEquals(2, stopped.getCandidatesEvaluated());
        assertEquals(2, stopped.getHits().size());
        assertEquals(RecallStopReason.DEADLINE, stopped.getStopReason());
    }

    @Test
    void noResultsAndCompletedTraceAreDistinctFromTopK() throws Exception {
        Fixture fixture = fixture();
        RecallService service = new RecallService(fixture.registry);
        RetrievalRequest query = request("BM25_ONLY", null);
        query.setQuery("not-present");
        RetrievalResponse empty = service.retrieve(query);
        assertEquals("NO_RESULT", empty.getTrace().getStopReason());
        assertTrue(empty.getEvidence().isEmpty());
        query.setQuery("Confucius");
        assertEquals("COMPLETED", service.retrieve(query).getTrace().getStopReason());
        query.setBudget(new RetrievalBudget(1, 1000, 10, 100));
        assertEquals("TOP_K", service.retrieve(query).getTrace().getStopReason());
    }

    @Test
    void legacyArtifactsWithoutIdentityRequireRebuild() throws Exception {
        Path oldBm25 = tempDir.resolve("bm25-v1");
        Files.createDirectories(oldBm25);
        try (org.apache.lucene.store.Directory directory = org.apache.lucene.store.FSDirectory.open(oldBm25);
             org.apache.lucene.analysis.standard.StandardAnalyzer analyzer = new org.apache.lucene.analysis.standard.StandardAnalyzer();
             org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(directory,
                 new org.apache.lucene.index.IndexWriterConfig(analyzer))) {
            writer.commit();
        }
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> new Bm25IndexReader(oldBm25, VERSION, VERSION));
        Path oldVector = tempDir.resolve("vector-v1.bin");
        try (java.io.DataOutputStream output = new java.io.DataOutputStream(Files.newOutputStream(oldVector))) {
            output.writeUTF("GEAFLOW-VECTOR-1");
        }
        assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> new VectorIndexReader(oldVector));
    }

    @Test
    void graphDeadlineAndEdgeCapAreObservableAndRepeatedPathsAreMerged() throws Exception {
        Fixture fixture = fixture();
        List<GraphEdgeRef> repeated = Arrays.asList(fixture.edges.get(0), fixture.edges.get(0), fixture.edges.get(1));
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.register(GRAPH, VERSION, VERSION, fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, repeated, true);
        RecallService service = new RecallService(registry);
        RetrievalResponse response = service.retrieve(request("GRAPH_ONLY", null));
        assertEquals(1, response.getEvidence().size());
        assertEquals(1, response.getEvidence().get(0).getPaths().size());
        RetrievalRequest capped = request("GRAPH_ONLY", null);
        capped.setBudget(new RetrievalBudget(1, 1000, 1, 100));
        assertTrue(service.retrieve(capped).getTrace().getEdgesExamined() <= 1);
        AtomicLong time = new AtomicLong();
        RecallService timed = new RecallService(registry, WeightedRrfConfig.DEFAULT, () -> time.getAndAdd(1_000_000L));
        capped.setBudget(new RetrievalBudget(1, 2, 2, 100));
        assertCode(RetrievalErrorCode.RETRIEVAL_TIMEOUT, () -> timed.retrieve(capped));
        time.set(0L);
        capped.setAllowPartialResults(true);
        assertEquals("DEADLINE", timed.retrieve(capped).getTrace().getStopReason());
    }

    @Test
    void zeroNormAndImpossibleShapeArtifactsAreTypedCorruption() throws Exception {
        Fixture fixture = fixture();
        Path artifact = tempDir.resolve("invalid-vector.bin");
        for (int count : new int[] {1, Integer.MAX_VALUE}) {
            try (java.io.DataOutputStream output = new java.io.DataOutputStream(Files.newOutputStream(artifact))) {
                output.writeUTF("GEAFLOW-VECTOR-2");
                output.writeUTF(GRAPH);
                output.writeUTF(VERSION);
                output.writeUTF(VERSION);
                output.writeUTF(org.apache.geaflow.ai.retrieval.index.ArtifactIdentity.fingerprint(fixture.chunks));
                output.writeUTF("fixture");
                output.writeUTF(VERSION);
                output.writeInt(2);
                output.writeInt(count);
                output.writeUTF("c1");
                output.writeUTF("doc-1");
                output.writeFloat(0.0F);
                output.writeFloat(0.0F);
            }
            assertCode(RetrievalErrorCode.INDEX_NOT_READY, () -> new VectorIndexReader(artifact));
        }
    }

    @Test
    void missingVectorAndCorruptBm25LoaderPathsRetainSuccessfulChannel() throws Exception {
        Fixture fixture = fixture();
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.load(GRAPH, VERSION, VERSION, tempDir.resolve("bm25/bm25-v1"), tempDir.resolve("absent"),
            fixture.chunks, fixture.entities, fixture.edges, "fixture", VERSION);
        readers.add(registry.get(GRAPH, VERSION, VERSION).getBm25());
        RetrievalRequest hybrid = request("HYBRID", Arrays.asList(1.0, 0.0));
        hybrid.setAllowPartialResults(true);
        RetrievalResponse partial = new RecallService(registry).retrieve(hybrid);
        assertEquals(Arrays.asList("VECTOR", "GRAPH"), partial.getDegradedChannels());
        assertFalse(partial.getEvidence().isEmpty());
        Path corruptBm25 = tempDir.resolve("corrupt-bm25");
        Files.createDirectories(corruptBm25);
        Files.write(corruptBm25.resolve("segments_1"), new byte[] {0, 1, 2});
        registry.load(GRAPH, VERSION, VERSION, corruptBm25, tempDir.resolve("vector/vector-v1.bin"),
            fixture.chunks, fixture.entities, fixture.edges, "fixture", VERSION);
        partial = new RecallService(registry).retrieve(hybrid);
        assertEquals(Arrays.asList("BM25", "GRAPH"), partial.getDegradedChannels());
        assertFalse(partial.getEvidence().isEmpty());
        assertEquals(RecallStopReason.INDEX_NOT_READY, partial.getTrace().getChannelStopReasons().get("BM25"));
    }

    @Test
    void directFacadeRequestsAlsoHaveIndependentExecutionState() throws Exception {
        Fixture fixture = fixture();
        RecallService direct = new RecallService(fixture.bm25, fixture.vector, fixture.chunks, fixture.entities, fixture.edges);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<RetrievalResponse>> calls = new ArrayList<>();
            for (int index = 0; index < 24; index++) {
                final String mode = index % 2 == 0 ? "HYBRID" : "GRAPH_ONLY";
                calls.add(executor.submit(() -> direct.retrieveWithTrace("Confucius",
                    mode.equals("HYBRID") ? new float[] {1.0F, 0.0F} : null, mode, 2, 4, 0L)));
            }
            for (int index = 0; index < calls.size(); index++) {
                RetrievalResponse response = calls.get(index).get();
                assertEquals(index % 2 == 0 ? Arrays.asList("BM25", "VECTOR", "GRAPH") : Collections.singletonList("GRAPH"),
                    response.getTrace().getSelectedChannels());
                assertEquals(index % 2 == 0 ? 4 : 2, response.getTrace().getTotalCandidatesEvaluated());
                assertEquals(index % 2 == 0 ? Collections.emptyList() : Collections.singletonList("GRAPH"),
                    response.getDegradedChannels());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Fixture fixture() throws Exception {
        List<TextChunk> chunks = Arrays.asList(
            new TextChunk("c1", "doc-1", 0, 0, 36, 6, "Confucius taught ethics and philosophy"),
            new TextChunk("c2", "doc-2", 0, 0, 25, 4, "Astronomy studies distant stars"),
            new TextChunk("c3", "doc-3", 0, 0, 35, 6, "Confucius and astronomy are studied"));
        IngestionContext context = context();
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("c1", new float[] {1.0F, 0.0F});
        vectors.put("c2", new float[] {0.0F, 1.0F});
        vectors.put("c3", new float[] {0.8F, 0.2F});
        Path bm25Path = tempDir.resolve("bm25");
        Path vectorPath = tempDir.resolve("vector");
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(bm25Path).build(context, chunks);
             org.apache.geaflow.ai.retrieval.index.IndexArtifact ignoredVector =
                 new OfflineVectorIndexBuilder(vectorPath, vectors, "fixture", VERSION)
                     .build(context, chunks)) {
            // Artifacts are published before readers are opened.
        }
        Bm25IndexReader bm25 = new Bm25IndexReader(bm25Path.resolve("bm25-v1"), VERSION, VERSION);
        readers.add(bm25);
        VectorIndexReader vector = new VectorIndexReader(vectorPath.resolve("vector-v1.bin"),
            VERSION, VERSION, "fixture", VERSION);
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", Collections.singletonList("kongzi"), "person",
                Collections.singletonList("c1")),
            new EntityRef("e2", "Astronomy", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Collections.singletonList("c3")),
            new GraphEdgeRef("edge-invalid", "bad", "e1", "e2", Collections.singletonList("missing")));
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.register(GRAPH, VERSION, VERSION, bm25, vector, chunks, entities, edges, true);
        return new Fixture(registry, bm25, vector, chunks, entities, edges);
    }

    private IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest(VERSION, "dataset", "revision", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 1L);
        return new IngestionContext(manifest, new GraphVersion(GRAPH, VERSION), "test");
    }

    private RetrievalRequest request(String mode, List<Double> vector) {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode(mode);
        request.setQueryVector(vector);
        request.setBudget(new RetrievalBudget(10, 1000, 10, 100));
        return request;
    }

    private static Evidence find(List<Evidence> evidence, String id) {
        return evidence.stream().filter(value -> id.equals(value.getEvidenceId())).findFirst()
            .orElseThrow(() -> new AssertionError("missing evidence " + id));
    }

    private static void assertCode(RetrievalErrorCode code, org.junit.jupiter.api.function.Executable executable) {
        RetrievalException exception = assertThrows(RetrievalException.class, executable);
        assertEquals(code, exception.getCode());
    }

    private static final class Fixture {
        private final RetrievalFixtureRegistry registry;
        private final Bm25IndexReader bm25;
        private final VectorIndexReader vector;
        private final List<TextChunk> chunks;
        private final List<EntityRef> entities;
        private final List<GraphEdgeRef> edges;

        private Fixture(RetrievalFixtureRegistry registry, Bm25IndexReader bm25,
                        VectorIndexReader vector, List<TextChunk> chunks,
                        List<EntityRef> entities, List<GraphEdgeRef> edges) {
            this.registry = registry;
            this.bm25 = bm25;
            this.vector = vector;
            this.chunks = chunks;
            this.entities = entities;
            this.edges = edges;
        }
    }
}
