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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that request-scoped audit identifiers never influence retrieval output.
 *
 * <p>The design requires that a fixed dataset version, configuration, policy/estimator version,
 * and random seed fully determine evidence order, graph paths, budget counters, and trace
 * contents. A caller-supplied or server-generated {@code requestId} is an audit identifier and
 * must therefore be excluded from every ordering, sampling, and identity-derivation input.
 */
class RetrievalDeterminismTest {
    private static final String GRAPH = "determinism-graph";
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
    void differingRequestIdsProduceIdenticalEvidenceOrderAndPaths() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse first = service.retrieve(request("GRAPH_ONLY", "request-alpha"));
        RetrievalResponse second = service.retrieve(request("GRAPH_ONLY", "request-beta"));

        assertEquals(evidenceIds(first), evidenceIds(second),
            "evidence order must not depend on requestId");
        assertEquals(first.getTrace().getTotalCandidatesEvaluated(),
            second.getTrace().getTotalCandidatesEvaluated(),
            "budget counters must not depend on requestId");
        assertEquals(graphPaths(first), graphPaths(second),
            "graph paths must not depend on requestId");
        assertEquals(graphPathVertices(first), graphPathVertices(second),
            "path vertices must not depend on requestId");
        assertEquals(first.getTrace().getChannelStopReasons(), second.getTrace().getChannelStopReasons(),
            "stop reasons must not depend on requestId");
    }

    @Test
    void graphTraceQueryIdIsStableAcrossRequestIds() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse first = service.retrieve(request("GRAPH_ONLY", "request-alpha"));
        RetrievalResponse second = service.retrieve(request("GRAPH_ONLY", "request-beta"));

        String firstQueryId = firstGraphTrace(first).getQueryId();
        String secondQueryId = firstGraphTrace(second).getQueryId();
        assertNotNull(firstQueryId, "graph trace must record a query id");
        assertEquals(firstQueryId, secondQueryId,
            "graph trace queryId must be derived from query identity, not requestId");
        assertNotEquals("request-alpha", firstQueryId, "queryId must not be the requestId");
    }

    @Test
    void requestIdRemainsRecordedForAudit() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse response = service.retrieve(request("GRAPH_ONLY", "request-alpha"));

        assertEquals("request-alpha", response.getRequestId(),
            "requestId must still be echoed for audit");
    }

    @Test
    void generatedRequestIdDoesNotChangeRetrievalOutput() throws Exception {
        RecallService service = new RecallService(fixture());
        RetrievalRequest withoutId = request("GRAPH_ONLY", null);

        RetrievalResponse first = service.retrieve(withoutId);
        RetrievalResponse second = service.retrieve(request("GRAPH_ONLY", null));

        assertNotNull(first.getRequestId());
        assertTrue(!first.getRequestId().equals(second.getRequestId()),
            "server-generated request ids are expected to differ");
        assertEquals(evidenceIds(first), evidenceIds(second),
            "server-generated requestId must not change evidence order");
        assertEquals(graphPaths(first), graphPaths(second));
    }

    private static GraphEvidenceTrace firstGraphTrace(RetrievalResponse response) {
        for (Evidence evidence : response.getEvidence()) {
            if (evidence.getGraphTraces() != null && !evidence.getGraphTraces().isEmpty()) {
                return evidence.getGraphTraces().get(0);
            }
        }
        throw new AssertionError("response has no graph evidence trace");
    }

    private static List<List<String>> graphPaths(RetrievalResponse response) {
        List<List<String>> paths = new ArrayList<>();
        for (Evidence evidence : response.getEvidence()) {
            if (evidence.getGraphTraces() == null) {
                continue;
            }
            for (GraphEvidenceTrace trace : evidence.getGraphTraces()) {
                paths.add(new ArrayList<>(trace.getPath().getVertexIds()));
            }
        }
        return paths;
    }

    private static List<List<String>> graphPathVertices(RetrievalResponse response) {
        List<List<String>> vertices = new ArrayList<>();
        for (Evidence evidence : response.getEvidence()) {
            if (evidence.getGraphTraces() == null) {
                continue;
            }
            for (GraphEvidenceTrace trace : evidence.getGraphTraces()) {
                List<String> entityIds = new ArrayList<>();
                for (org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef vertex
                    : trace.getPath().getVertices()) {
                    entityIds.add(vertex.getEntityId());
                }
                vertices.add(entityIds);
            }
        }
        return vertices;
    }

    private static List<String> evidenceIds(RetrievalResponse response) {
        List<String> ids = new ArrayList<>();
        for (Evidence evidence : response.getEvidence()) {
            ids.add(evidence.getEvidenceId());
        }
        return ids;
    }

    private RetrievalChannelRegistry fixture() throws Exception {
        List<TextChunk> chunks = Arrays.asList(
            new TextChunk("c1", "doc-1", 0, 0, 36, 6,
                "Confucius taught ethics and philosophy", null, null, "https://example.test/doc-1"),
            new TextChunk("c2", "doc-2", 0, 0, 25, 4,
                "Astronomy studies distant stars", null, null, "https://example.test/doc-2"),
            new TextChunk("c3", "doc-3", 0, 0, 35, 6,
                "Confucius and astronomy are studied", null, null, "https://example.test/doc-3"),
            new TextChunk("c4", "doc-4", 0, 0, 34, 6,
                "Ritual practice shaped early thought", null, null, "https://example.test/doc-4"),
            new TextChunk("c5", "doc-5", 0, 0, 33, 6,
                "Music accompanied the ceremonies", null, null, "https://example.test/doc-5"));
        IngestionContext context = context();
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("c1", new float[] {1.0F, 0.0F});
        vectors.put("c2", new float[] {0.0F, 1.0F});
        vectors.put("c3", new float[] {0.8F, 0.2F});
        vectors.put("c4", new float[] {0.6F, 0.4F});
        vectors.put("c5", new float[] {0.4F, 0.6F});
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
            new EntityRef("e2", "Astronomy", "topic"),
            new EntityRef("e3", "Ritual", "topic"),
            new EntityRef("e4", "Music", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Collections.singletonList("c3")),
            new GraphEdgeRef("edge-2", "mentions", "e2", "e1", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-3", "studies", "e1", "e3", Collections.singletonList("c4")),
            new GraphEdgeRef("edge-4", "studies", "e1", "e4", Collections.singletonList("c5")));
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, bm25, vector, chunks, entities, edges, true);
        return registry;
    }

    private IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest(VERSION, "dataset", "revision", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 1L);
        return new IngestionContext(manifest, new GraphVersion(GRAPH, VERSION), "test");
    }

    private RetrievalRequest request(String mode, String requestId) {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode(mode);
        request.setRequestId(requestId);
        request.setBudget(new RetrievalBudget(10, 1000, 10, 100));
        return request;
    }
}
