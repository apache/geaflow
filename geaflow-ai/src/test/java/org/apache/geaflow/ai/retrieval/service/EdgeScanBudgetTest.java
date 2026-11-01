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
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that graph edge scanning is budgeted independently from candidate evaluation.
 *
 * <p>The design reports edges examined as a first-class secondary cost so that "scan a lot, return
 * a little" diffusion behaviour is measurable and separately tunable. Binding the edge-scan bound
 * to the candidate bound collapses those two dimensions into one and makes equal-budget comparison
 * between methods ill-defined.
 */
class EdgeScanBudgetTest {
    private static final String GRAPH = "edge-budget-graph";
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
    void edgeScanBudgetBoundsScanWorkIndependentlyOfCandidateBudget() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse response = service.retrieve(request(100, 3));

        assertEquals(3, response.getTrace().getEdgesExamined(),
            "edge scans must stop at the independent edge-scan bound");
        assertEquals(RecallStopReason.EDGE_SCAN_LIMIT,
            response.getTrace().getChannelStopReasons().get("GRAPH"),
            "the graph channel must report an edge-scan stop, not a candidate stop");
    }

    @Test
    void edgeScanBudgetDoesNotChangeWhenCandidateBudgetChanges() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse narrow = service.retrieve(request(2, 4));

        assertEquals(4, narrow.getTrace().getEdgesExamined(),
            "edge scans must follow maxEdgeScans even when maxCandidates is smaller");
        assertNotEquals(narrow.getTrace().getTotalCandidatesEvaluated(), 0,
            "candidate evaluation must still happen independently");
    }

    @Test
    void omittedEdgeScanBudgetFallsBackToServerDefaultNotCandidateBudget() throws Exception {
        RecallService service = new RecallService(fixture());
        RetrievalRequest request = request(100, 100);
        request.getBudget().setMaxEdgeScans(4);

        RetrievalResponse explicit = service.retrieve(request);
        assertEquals(4, explicit.getTrace().getEdgesExamined());

        RetrievalRequest omitted = request(100, 100);
        omitted.getBudget().setMaxEdgeScans(100);
        RetrievalResponse bounded = service.retrieve(omitted);
        assertTrue(bounded.getTrace().getEdgesExamined() > 4,
            "an explicit larger edge budget must be honoured, proving it is not pinned to "
                + "the candidate budget");
    }

    @Test
    void effectiveBudgetEchoesTheEdgeScanBound() throws Exception {
        RecallService service = RecallService.fromProperties(fixture(),
            org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig.DEFAULT,
            System::nanoTime, properties());

        RetrievalResponse response = service.retrieve(request(9, 7));

        assertEquals(7, response.getEffectiveBudget().getMaxEdgeScans(),
            "the effective budget must echo the applied edge-scan bound");
    }

    private RetrievalProperties properties() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setDefaultMaxEdgeScans(5);
        properties.setMaxEdgeScans(5000);
        return properties;
    }

    private RetrievalFixtureRegistry fixture() throws Exception {
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
                "Music accompanied the ceremonies", null, null, "https://example.test/doc-5"),
            new TextChunk("c6", "doc-6", 0, 0, 32, 6,
                "Calligraphy preserved the classics", null, null, "https://example.test/doc-6"));
        IngestionContext context = context();
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("c1", new float[] {1.0F, 0.0F});
        vectors.put("c2", new float[] {0.0F, 1.0F});
        vectors.put("c3", new float[] {0.8F, 0.2F});
        vectors.put("c4", new float[] {0.6F, 0.4F});
        vectors.put("c5", new float[] {0.4F, 0.6F});
        vectors.put("c6", new float[] {0.2F, 0.8F});
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
            new EntityRef("e4", "Music", "topic"),
            new EntityRef("e5", "Calligraphy", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Collections.singletonList("c3")),
            new GraphEdgeRef("edge-2", "mentions", "e2", "e1", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-3", "studies", "e1", "e3", Collections.singletonList("c4")),
            new GraphEdgeRef("edge-4", "studies", "e1", "e4", Collections.singletonList("c5")),
            new GraphEdgeRef("edge-5", "studies", "e1", "e5", Collections.singletonList("c6")));
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.register(GRAPH, VERSION, VERSION, bm25, vector, chunks, entities, edges, true);
        return registry;
    }

    private IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest(VERSION, "dataset", "revision", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 1L);
        return new IngestionContext(manifest, new GraphVersion(GRAPH, VERSION), "test");
    }

    private RetrievalRequest request(int maxCandidates, int maxEdgeScans) {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode("GRAPH_ONLY");
        RetrievalBudget budget = new RetrievalBudget(Math.min(10, maxCandidates), 1000,
            maxCandidates, 100);
        budget.setMaxEdgeScans(maxEdgeScans);
        request.setBudget(budget);
        return request;
    }
}