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
package org.apache.geaflow.ai.retrieval.api.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig;
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
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.service.RecallService;
import org.apache.geaflow.ai.retrieval.service.RetrievalFixtureRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the five retrieval modes named by the design document as distinct API values.
 *
 * <p>The design specifies {@code BM25_ONLY}, {@code VECTOR_ONLY}, {@code GRAPH_ONLY},
 * {@code FIXED_HYBRID} and {@code ADAPTIVE_HYBRID}. Treating "adaptive" as an out-of-band
 * combination of {@code HYBRID} plus a non-cascaded execution mode means a caller cannot ask for
 * adaptive routing directly, and it makes the adaptive contract impossible to pin in a request.
 */
class RetrievalModeEnumTest {
    private static final String GRAPH = "mode-enum-graph";
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
    void exposesTheFiveDocumentedModes() {
        List<String> names = new ArrayList<>();
        for (RetrievalMode mode : RetrievalMode.values()) {
            names.add(mode.name());
        }
        for (String required : Arrays.asList("BM25_ONLY", "VECTOR_ONLY", "GRAPH_ONLY",
            "FIXED_HYBRID", "ADAPTIVE_HYBRID")) {
            assertTrue(names.contains(required), required + " must be a declared mode: " + names);
        }
    }

    @Test
    void adaptiveAndFixedHybridAreDistinctModes() {
        assertNotEquals(RetrievalMode.FIXED_HYBRID, RetrievalMode.ADAPTIVE_HYBRID);
        assertEquals(RetrievalMode.HYBRID, RetrievalMode.FIXED_HYBRID.canonical());
        assertEquals(RetrievalMode.HYBRID, RetrievalMode.ADAPTIVE_HYBRID.canonical(),
            "both hybrid modes execute the hybrid channel set");
    }

    @Test
    void adaptiveModeRunsTheRoutingPolicyEvenUnderCascadedExecution() throws Exception {
        // The design states adaptive hybrid should default to cascaded execution, so cascaded must
        // not be the one execution mode where routing silently does not run.
        RecallService service = RecallService.fromProperties(fixture(), WeightedRrfConfig.DEFAULT,
            System::nanoTime, new RetrievalProperties());
        RetrievalRequest request = request(RetrievalMode.ADAPTIVE_HYBRID.name());
        request.setExecutionMode("CASCADED");

        RetrievalResponse response = service.retrieve(request);

        assertEquals("deterministic-rule", response.getTrace().getPolicyId(),
            "adaptive routing must run under cascaded execution");
        assertEquals(RetrievalMode.ADAPTIVE_HYBRID, response.getTrace().getRequestedMode());
    }

    @Test
    void fixedHybridModeSkipsTheRoutingPolicy() throws Exception {
        RecallService service = RecallService.fromProperties(fixture(), WeightedRrfConfig.DEFAULT,
            System::nanoTime, new RetrievalProperties());
        RetrievalRequest request = request(RetrievalMode.FIXED_HYBRID.name());
        request.setExecutionMode("SEQUENTIAL");

        RetrievalResponse response = service.retrieve(request);

        assertEquals(null, response.getTrace().getPolicyId(),
            "fixed hybrid must not consult the routing policy");
        assertEquals(RetrievalMode.FIXED_HYBRID, response.getTrace().getRequestedMode());
    }

    private RetrievalRequest request(String mode) {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode(mode);
        request.setQueryVector(Arrays.asList(1.0, 0.0));
        request.setBudget(new RetrievalBudget(5, 1000, 10, 200));
        return request;
    }

    private RetrievalFixtureRegistry fixture() throws Exception {
        List<TextChunk> chunks = Arrays.asList(
            chunk("c1", "doc-1", "Confucius taught ethics and philosophy"),
            chunk("c2", "doc-2", "Astronomy studies distant stars"),
            chunk("c3", "doc-3", "Confucius and astronomy are studied"));
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
            new EntityRef("e1", "Confucius", Arrays.asList("kongzi"), "person", Arrays.asList("c1")),
            new EntityRef("e2", "Astronomy", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Arrays.asList("c3")));
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        registry.register(GRAPH, VERSION, VERSION, bm25, vector, chunks, entities, edges, true);
        return registry;
    }

    private static TextChunk chunk(String id, String documentId, String text) {
        return new TextChunk(id, documentId, 0, 0, text.length(), 6, text, null, null,
            "https://example.test/" + documentId);
    }

    private IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest(VERSION, "dataset", "revision", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 1L);
        return new IngestionContext(manifest, new GraphVersion(GRAPH, VERSION), "test");
    }

    @Test
    void cascadedAdaptiveHonoursTheRoutingBudgetsInsteadOfADefaultProbeSplit() throws Exception {
        RecallService service = RecallService.fromProperties(fixture(), WeightedRrfConfig.DEFAULT,
            System::nanoTime, new RetrievalProperties());
        RetrievalRequest request = request(RetrievalMode.ADAPTIVE_HYBRID.name());
        request.setExecutionMode("CASCADED");

        RetrievalResponse response = service.retrieve(request);

        // The routing plan owns the channel budgets. A cascaded run must execute that plan's
        // allocation, otherwise the recorded plan is decorative and the cascade silently uses a
        // different split than the one the trace reports.
        String policyId = response.getTrace().getPolicyId();
        assertEquals("deterministic-rule", policyId);
        int budgetSum = response.getTrace().getChannelBudgets().values().stream()
            .mapToInt(Integer::intValue).sum();
        assertEquals(response.getTrace().getEffectiveCandidateBudget(), budgetSum,
            "the executed channel budgets must be the routed plan's budgets: "
                + response.getTrace().getChannelBudgets());
        assertEquals(RecallStageStatus.SUCCESS,
            response.getTrace().getChannelStatuses().get("GRAPH"),
            "cascade must still run the graph stage when the plan funds it");
    }

    @Test
    void cascadedAdaptiveStillReplansAtTheStageBoundary() throws Exception {
        // The design allows bounded replanning at configured stage boundaries. A cascaded adaptive
        // request has exactly such a boundary between the text probe and the graph stage, so the
        // policy must be consulted there instead of the cascade using a hardcoded decision.
        CountingPolicy policy = new CountingPolicy();
        RecallService service = new RecallService(fixture(), WeightedRrfConfig.DEFAULT,
            System::nanoTime,
            org.apache.geaflow.ai.retrieval.execution.RetrievalStrategyFactory.tokenEstimator("chars4-v1"),
            org.apache.geaflow.ai.retrieval.execution.RetrievalStrategyFactory.reranker("deterministic-reranker-v1"),
            policy, 1);
        RetrievalRequest request = request(RetrievalMode.ADAPTIVE_HYBRID.name());
        request.setExecutionMode("CASCADED");

        service.retrieve(request);

        assertTrue(policy.calls > 1,
            "the routing policy must be consulted after the probe as well as before it; calls="
                + policy.calls);
    }

    /** Routing policy that counts how often planning is invoked. */
    private static final class CountingPolicy
        implements org.apache.geaflow.ai.retrieval.execution.RoutingPolicy {
        private final org.apache.geaflow.ai.retrieval.execution.DeterministicRulePolicy delegate =
            new org.apache.geaflow.ai.retrieval.execution.DeterministicRulePolicy();
        private int calls;

        @Override
        public org.apache.geaflow.ai.retrieval.execution.RetrievalPlan plan(
            org.apache.geaflow.ai.retrieval.execution.RoutingContext context,
            org.apache.geaflow.ai.retrieval.execution.RetrievalProgress progress) {
            calls++;
            return delegate.plan(context, progress);
        }

        @Override
        public boolean supports(RetrievalMode mode) {
            return delegate.supports(mode.canonical());
        }

        @Override
        public String getPolicyId() {
            return delegate.getPolicyId();
        }

        @Override
        public String getPolicyVersion() {
            return delegate.getPolicyVersion();
        }

        @Override
        public Map<String, String> getParameters() {
            return delegate.getParameters();
        }
    }
}