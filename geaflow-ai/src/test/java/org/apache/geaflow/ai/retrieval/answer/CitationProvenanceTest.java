/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 */
package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.junit.jupiter.api.Test;

/**
 * Pins the design requirement that every citation is traceable to query, mode, seed, relation path,
 * evidence and source document.
 *
 * <p>The original citation carried only four fields, so an answer could be quoted but never audited
 * back to the search that produced it.
 */
class CitationProvenanceTest {

    @Test
    void textCitationCarriesQueryModeAndSourceDocument() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris", null, null,
            "https://example.test/d1");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(chunk), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "https://example.test/d1")),
            new HashMap<>(), null, 1);

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where is Paris", 2, "req-1", "gv", "iv", "graph",
                "BM25_ONLY"), Collections.singletonList(evidence));

        Citation citation = response.getCitations().get(0);
        assertNotNull(citation.getQueryId());
        assertEquals("BM25_ONLY", citation.getMode());
        assertEquals("d1", citation.getDocumentId());
        assertEquals("https://example.test/d1", citation.getSourceUri());
        assertEquals(0, citation.getHop());
        assertTrue(citation.getRelationPath().isEmpty());
    }

    @Test
    void graphCitationCarriesSeedAndRelationPath() {
        TextChunk chunk = new TextChunk("c3", "doc-3", 0, 0, 9, 3, "Confucius");
        EntityRef seed = new EntityRef("e1", "Confucius", Arrays.asList("kongzi"), "person",
            Arrays.asList("c3"));
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "studies", "e1", "e2", Arrays.asList("c3"));
        GraphVertexRef seedVertex = new GraphVertexRef("person", "e1", "e1");
        GraphVertexRef targetVertex = new GraphVertexRef("topic", "e2", "e2");
        GraphPathRef path = new GraphPathRef(Arrays.asList("e1", "e2"), Arrays.asList("edge-1"),
            1, false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q-42", RetrievalMode.GRAPH_ONLY,
            seedVertex, "ALIAS", 0.94D, "gv", path, Arrays.asList("c3"), Arrays.asList(seedVertex, targetVertex),
            Collections.singletonList(edge));
        java.util.Map<String, org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore> scores =
            new java.util.HashMap<>();
        Evidence evidence = new Evidence("ev-203", EvidenceKind.GRAPH_PATH, "Confucius",
            Collections.singletonList(chunk), Arrays.asList(seed), Collections.singletonList(path),
            Collections.singletonList(new SourceRef("doc-3", "https://example.test/doc-3")),
            scores, 0.84D, 0.84D, 1, Arrays.asList(trace));

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("who taught ethics", 10, "req-2", "gv", "iv", "graph",
                "GRAPH_ONLY"), Collections.singletonList(evidence));

        Citation citation = response.getCitations().get(0);
        assertEquals("q-42", citation.getQueryId());
        assertEquals("e1", citation.getSeedEntityId());
        assertEquals(1, citation.getHop());
        assertEquals(Arrays.asList("studies"), citation.getRelationPath());
        assertEquals("doc-3", citation.getDocumentId());
        assertEquals("https://example.test/doc-3", citation.getSourceUri());
        assertTrue(citation.hasFullProvenance(),
            "graph citation must report query, mode, seed, path and source document");
    }

    @Test
    void legacyFourArgumentCitationIsRetainedButReportedAsIncomplete() {
        Citation legacy = new Citation("ev-1", "src-1", "c1", "hop=1;edges=e1");

        assertEquals("ev-1", legacy.getEvidenceId());
        assertFalse(legacy.hasFullProvenance(),
            "a legacy citation must not claim provenance it does not have");
    }

    @Test
    void citationRejectsNegativeHop() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> new Citation("ev", "src", "c", "p", "q", "MODE", "seed", "doc", "uri",
                new ArrayList<>(), -1));
    }

    @Test
    void provenanceIsSerializedOnTheWire() {
        Citation citation = new Citation("ev-9", "doc-9", "c9", "hop=2",
            "q-9", "ADAPTIVE_HYBRID", "e-17", "doc-9", "https://example.test/doc-9",
            Arrays.asList("rel-81", "rel-96"), 2);
        AnswerResponse response = new AnswerResponse("text", null, "text", 1,
            Collections.singletonList(citation));
        String json = org.apache.geaflow.ai.retrieval.codec.AnswerApiJson.toJson(response);

        assertTrue(json.contains("\"queryId\":\"q-9\""), json);
        assertTrue(json.contains("\"mode\":\"ADAPTIVE_HYBRID\""), json);
        assertTrue(json.contains("\"seedEntityId\":\"e-17\""), json);
        assertTrue(json.contains("\"documentId\":\"doc-9\""), json);
        assertTrue(json.contains("\"sourceUri\":\"https://example.test/doc-9\""), json);
        assertTrue(json.contains("\"relationPath\":[\"rel-81\",\"rel-96\"]"), json);
        assertTrue(json.contains("\"fullProvenance\":true"), json);
    }

    @Test
    void everyCitationOfAMultiChunkAnswerCarriesProvenance() {
        TextChunk first = new TextChunk("c1", "d1", 0, 0, 3, 2, "one", null, null, "uri-1");
        TextChunk second = new TextChunk("c2", "d1", 0, 0, 3, 2, "two", null, null, "uri-1");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "one two",
            Arrays.asList(first, second), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "uri-1")), new HashMap<>(), null, 1);

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("q", 8, "req-3", "gv", "iv", "graph", "FIXED_HYBRID"),
                Collections.singletonList(evidence));

        List<Citation> citations = response.getCitations();
        assertEquals(2, citations.size());
        for (Citation citation : citations) {
            assertNotNull(citation.getQueryId());
            assertEquals("FIXED_HYBRID", citation.getMode());
            assertEquals("d1", citation.getDocumentId());
        }
    }
}