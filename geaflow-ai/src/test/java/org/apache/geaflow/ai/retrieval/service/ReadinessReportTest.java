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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.junit.jupiter.api.Test;

/**
 * Verifies that readiness is reported per artifact component.
 *
 * <p>{@code /ready} previously answered "some READY fixture exists", so a bundle missing its BM25
 * or vector artifact still reported ready while single-channel requests failed later with
 * {@code INDEX_NOT_READY}. The design asks readiness to cover the graph, BM25 and vector artifacts.
 */
class ReadinessReportTest {
    private static final String GRAPH = "ready-graph";
    private static final String VERSION = "v1";

    @Test
    void reportsMissingBm25ArtifactInTheReason() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertEquals("BM25_NOT_READY", report.getComponents().get("bm25"),
            "an absent BM25 artifact must be visible as a component failure");
        assertFalse(report.isReady());
        assertEquals("BM25_NOT_READY", report.getReason());
    }

    @Test
    void componentsAreReportedIndividually() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertEquals("READY", report.getComponents().get("graph"));
        assertEquals("BM25_NOT_READY", report.getComponents().get("bm25"));
        assertEquals("VECTOR_NOT_READY", report.getComponents().get("vector"));
    }

    @Test
    void unknownVersionIsNotReady() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, "v9", "v9");

        assertFalse(report.isReady());
        assertEquals("GRAPH_NOT_READY", report.getReason());
    }

    @Test
    void unreadyFixtureIsNotReady() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), false);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertFalse(report.isReady(), "a staging fixture must not report ready");
    }

    private static List<TextChunk> chunks() {
        return Collections.singletonList(new TextChunk("c1", "doc-1", 0, 0, 10, 2,
            "Confucius", null, null, "https://example.test/doc-1"));
    }

    private static List<EntityRef> entities() {
        return Collections.singletonList(new EntityRef("e1", "Confucius", "person"));
    }

    private static List<GraphEdgeRef> edges() {
        return Collections.emptyList();
    }
}