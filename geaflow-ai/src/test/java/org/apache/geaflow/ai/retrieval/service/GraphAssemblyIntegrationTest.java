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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the graph expansion controls configured by an operator actually reach the graph
 * retriever when a request is served through {@link RecallService}.
 *
 * <p>These controls existed on {@code GraphRetrievalConfig} before, but nothing in the production
 * assembly ever set them: sampling, relation-type priority, the marginal-yield floor, and the
 * association-score floor silently ran at their neutral defaults. Unit tests passed because they
 * passed the values explicitly, so the design intent was never exercised end to end.
 */
class GraphAssemblyIntegrationTest {
    private static final String GRAPH = "assembly-graph";
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
    void configuredSampleRateReducesNeighborSamplingInProductionPath() throws Exception {
        RetrievalProperties sampled = properties();
        sampled.setGraphSampleRate(0.0D);
        RetrievalResponse starved = serve(sampled);

        RetrievalProperties saturated = properties();
        saturated.setGraphSampleRate(1.0D);
        RetrievalResponse full = serve(saturated);

        assertEquals(0, starved.getTrace().getNeighborsSampled(),
            "a zero sample rate must suppress every neighbour draw");
        assertTrue(full.getTrace().getNeighborsSampled() > 0,
            "a full sample rate must draw neighbours");
        assertTrue(full.getEvidence().size() > starved.getEvidence().size(),
            "sampling must change the evidence produced, proving it reaches the retriever");
    }

    @Test
    void configuredRelationPriorityReordersNeighboursInProductionPath() throws Exception {
        RetrievalProperties prefersStudies = properties();
        prefersStudies.setGraphRelationPriorities(mapOf("studies", 10.0D, "mentions", 0.1D));
        RetrievalResponse first = serve(prefersStudies, 2);

        RetrievalProperties prefersMentions = properties();
        prefersMentions.setGraphRelationPriorities(mapOf("studies", 0.1D, "mentions", 10.0D));
        RetrievalResponse second = serve(prefersMentions, 2);

        assertNotEquals(evidenceIds(first), evidenceIds(second),
            "relation-type priority must change which edges survive the neighbour cap");
    }

    @Test
    void configuredMinimumMarginalYieldStopsExpansionInProductionPath() throws Exception {
        RetrievalProperties strict = properties();
        strict.setGraphMinMarginalYield(0.9D);

        RetrievalResponse response = serve(strict);

        assertEquals(RecallStopReason.MARGINAL_YIELD_LIMIT,
            response.getTrace().getChannelStopReasons().get("GRAPH"),
            "the marginal-yield floor must be able to stop expansion in production");
    }

    @Test
    void configuredMinimumAssociationScoreFiltersWeakEdgesInProductionPath() throws Exception {
        RetrievalProperties strict = properties();
        strict.setGraphMinAssociationScore(0.99D);
        strict.setGraphMinAssociationSurvivalRate(0.5D);

        RetrievalResponse filtered = serve(strict, 100);

        assertTrue(filtered.getEvidence().size() < allEdgesEvidenceSize(),
            "an association floor must drop weak edges in production; filtered="
                + filtered.getEvidence().size() + " unfiltered=" + allEdgesEvidenceSize()
                + " reason=" + filtered.getTrace().getChannelStopReasons().get("GRAPH")
                + " filteredByAssociation=" + filtered.getTrace().getGraphCandidatesProduced());
        assertEquals(RecallStopReason.LOW_ASSOCIATION_SCORE,
            filtered.getTrace().getChannelStopReasons().get("GRAPH"));
    }

    @Test
    void defaultPropertiesKeepTheGraphChannelProductive() throws Exception {
        RetrievalResponse response = serve(properties());

        assertTrue(!response.getEvidence().isEmpty(),
            "neutral defaults must not starve the graph channel");
    }

    private int allEdgesEvidenceSize() throws Exception {
        return serve(properties(), 100).getEvidence().size();
    }

    private static Map<String, Double> mapOf(String key, double first, String other, double second) {
        Map<String, Double> values = new HashMap<>();
        values.put(key, first);
        values.put(other, second);
        return values;
    }

    private RetrievalResponse serve(RetrievalProperties properties) throws Exception {
        return serve(properties, 100);
    }

    private RetrievalResponse serve(RetrievalProperties properties, int maxEdgeScans) throws Exception {
        RetrievalFixtureRegistry registry = fixture();
        RecallService service = RecallService.fromProperties(registry, WeightedRrfConfig.DEFAULT,
            System::nanoTime, properties);
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode("GRAPH_ONLY");
        RetrievalBudget budget = new RetrievalBudget(10, 1000, 10, 100);
        budget.setMaxEdgeScans(maxEdgeScans);
        request.setBudget(budget);
        return service.retrieve(request);
    }

    private static RetrievalProperties properties() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setGraphSampleRate(1.0D);
        properties.setGraphMinMarginalYield(0.0D);
        properties.setGraphMinAssociationScore(0.0D);
        properties.setGraphMinAssociationSurvivalRate(0.0D);
        properties.setGraphRelationPriorities(new HashMap<>());
        return properties;
    }

    private static List<String> evidenceIds(RetrievalResponse response) {
        List<String> ids = new ArrayList<>();
        response.getEvidence().forEach(evidence -> ids.add(evidence.getEvidenceId()));
        return ids;
    }

    private RetrievalFixtureRegistry fixture() throws Exception {
        List<TextChunk> chunks = Arrays.asList(
            chunk("c1", "doc-1", "Confucius taught ethics and philosophy"),
            chunk("c2", "doc-2", "Astronomy studies distant stars"),
            chunk("c3", "doc-3", "Confucius and astronomy are studied"),
            chunk("c4", "doc-4", "Ritual practice shaped early thought"),
            chunk("c5", "doc-5", "Music accompanied the ceremonies"),
            chunk("c6", "doc-6", "Calligraphy preserved the classics"));
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
            new GraphEdgeRef("edge-5", "mentions", "e1", "e5", Collections.singletonList("c6")));
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
}