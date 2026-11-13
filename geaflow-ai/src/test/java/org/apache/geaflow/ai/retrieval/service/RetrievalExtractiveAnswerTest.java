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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
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
 * Pins that a retrieval response carries an offline extractive answer.
 *
 * <p>The design lists "extractive Answer EM/F1" as a required metric, which is only computable if
 * the retrieval response actually carries a predicted answer. Before this contract existed the
 * answer columns were structurally null for every run because the response had no answer field.
 */
class RetrievalExtractiveAnswerTest {
    private static final String GRAPH = "answer-contract-graph";
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
    void successfulRetrievalCarriesAnExtractiveAnswer() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse response = service.retrieve(request("BM25_ONLY"));

        assertFalse(response.getEvidence().isEmpty(), "fixture must return evidence");
        assertNotNull(response.getAnswer(), "response must carry an extracted answer");
        assertTrue(response.getAnswer().contains("Confucius"),
            "extracted answer must be text taken from returned evidence, was: " + response.getAnswer());
        assertNull(response.getAnswerAbstentionReason(),
            "a non-empty answer must not also report abstention");
    }

    @Test
    void answerTextIsDrawnFromReturnedEvidenceNotGenerated() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse response = service.retrieve(request("BM25_ONLY"));

        List<String> returnedTexts = new ArrayList<>();
        response.getEvidence().forEach(value -> value.getChunks()
            .forEach(chunk -> returnedTexts.add(chunk.getText())));
        assertFalse(returnedTexts.isEmpty(), "fixture must return chunk text");
        // The extractive answer is the concatenation of whole selected chunks, so every line of it
        // must be byte-identical to a chunk that the response actually returned. A generated or
        // paraphrased answer would fail this check.
        for (String line : response.getAnswer().split("\\R")) {
            if (line.trim().isEmpty()) {
                continue;
            }
            assertTrue(returnedTexts.contains(line),
                "answer line must be a verbatim returned chunk, was: " + line);
        }
    }

    @Test
    void graphOnlyWithoutAnchorAbstainsWithTypedReason() throws Exception {
        RecallService service = new RecallService(fixture());
        RetrievalRequest request = request("GRAPH_ONLY");
        request.setQuery("unrelated entity");

        RetrievalResponse response = service.retrieve(request);

        assertTrue(response.getEvidence().isEmpty(), "graph-only without anchor returns no evidence");
        assertNull(response.getAnswer(), "no evidence means no answer");
        assertNotNull(response.getAnswerAbstentionReason(),
            "an empty result must state why no answer was produced");
    }

    @Test
    void answerSurvivesJsonRoundTrip() throws Exception {
        RecallService service = new RecallService(fixture());

        RetrievalResponse response = service.retrieve(request("BM25_ONLY"));
        String json = RetrievalApiJson.toJson(response);
        RetrievalResponse restored = RetrievalApiJson.parseResponse(json);

        assertEquals(response.getAnswer(), restored.getAnswer());
        assertEquals(response.getAnswerAbstentionReason(), restored.getAnswerAbstentionReason());
    }

    @Test
    void answeredResponseWithoutEvidenceIsRejected() throws Exception {
        RecallService service = new RecallService(fixture());
        RetrievalResponse response = service.retrieve(request("BM25_ONLY"));
        response.setEvidence(new ArrayList<>());

        org.apache.geaflow.ai.retrieval.api.model.RetrievalException error =
            org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
                () -> RetrievalApiJson.toJson(response));

        assertTrue(error.getMessage().contains("evidence"), error.getMessage());
    }

    @Test
    void answeredAndAbstainedResponseIsRejected() throws Exception {
        RecallService service = new RecallService(fixture());
        RetrievalResponse response = service.retrieve(request("BM25_ONLY"));
        response.setAnswerAbstentionReason("NO_EVIDENCE");

        org.apache.geaflow.ai.retrieval.api.model.RetrievalException error =
            org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
                () -> RetrievalApiJson.toJson(response));

        assertTrue(error.getMessage().contains("abstain"), error.getMessage());
    }

    private RetrievalRequest request(String mode) {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery("Confucius");
        request.setMode(mode);
        request.setBudget(new RetrievalBudget(5, 1000, 10, 200));
        if ("HYBRID".equals(mode) || "VECTOR_ONLY".equals(mode)) {
            request.setQueryVector(Arrays.asList(1.0, 0.0));
        }
        return request;
    }

    private RetrievalChannelRegistry fixture() throws Exception {
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
            new EntityRef("e1", "Confucius", Arrays.asList("kongzi"), "person",
                Arrays.asList("c1")),
            new EntityRef("e2", "Astronomy", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Arrays.asList("c3")));
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
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