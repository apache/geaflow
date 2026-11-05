/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 */
package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/** Pins citation provenance from retrieval through extractive answers. */
class CitationProvenanceTest {

    @Test
    void textCitationCarriesQueryModeAndSourceDocument() {
        TextChunk chunk = chunk("c1", "d1", "Paris", 2);
        Evidence evidence = textEvidence("e1", "Paris", chunk);
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where is Paris", 2,
            "req-1", "gv", "iv", "graph", "BM25_ONLY"), Collections.singletonList(evidence));
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
        TextChunk chunk = chunk("c3", "doc-3", "Confucius", 3);
        EntityRef seed = new EntityRef("e1", "Confucius", Arrays.asList("kongzi"), "person",
            Collections.singletonList("c3"), "dataset", "v1", "test", "hash-doc-3");
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "studies", "e1", "e2",
            Collections.singletonList("c3"), "dataset", "v1", "test", "hash-doc-3");
        GraphVertexRef seedVertex = new GraphVertexRef("person", "e1", "e1",
            "dataset", "v1", "test", "hash-doc-3");
        GraphVertexRef targetVertex = new GraphVertexRef("topic", "e2", "e2",
            "dataset", "v1", "test", "hash-doc-3");
        GraphPathRef path = new GraphPathRef(Arrays.asList(seedVertex, targetVertex),
            Collections.singletonList(edge), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q-42", RetrievalMode.GRAPH_ONLY,
            "ALIAS", 0.94D, "gv", path, Collections.singletonList("c3"));
        Evidence evidence = new Evidence("ev-203", EvidenceKind.GRAPH_PATH, "Confucius",
            Collections.singletonList(chunk), Collections.singletonList(seed),
            Collections.singletonList(new SourceRef("doc-3", "https://example.test/doc-3",
                null, null, "dataset", "v1", "test", "hash-doc-3")),
            new HashMap<>(), 0.84D, 0.84D, 1, Collections.singletonList(trace),
            Evidence.CURRENT_SCHEMA_VERSION, "dataset", "v1", "test", "gv", "iv");
        AnswerResponse response = consumer().synthesize(new AnswerRequest("who taught ethics", 10,
            "req-2", "gv", "iv", "graph", "GRAPH_ONLY"), Collections.singletonList(evidence));
        Citation citation = response.getCitations().get(0);
        assertEquals("q-42", citation.getQueryId());
        assertEquals("e1", citation.getSeedEntityId());
        assertEquals(1, citation.getHop());
        assertEquals(Collections.singletonList("studies"), citation.getRelationPath());
        assertEquals("doc-3", citation.getDocumentId());
        assertEquals("https://example.test/doc-3", citation.getSourceUri());
        assertTrue(citation.hasFullProvenance());
    }

    @Test
    void legacyFourArgumentCitationIsRetainedButReportedAsIncomplete() {
        Citation legacy = new Citation("ev-1", "src-1", "c1", "hop=1;edges=e1");
        assertEquals("ev-1", legacy.getEvidenceId());
        assertFalse(legacy.hasFullProvenance());
    }

    @Test
    void citationRejectsNegativeHop() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> new Citation("ev", "src", "c", "p", "q", "MODE", "seed", "doc", "uri",
                Collections.emptyList(), -1));
    }

    @Test
    void provenanceIsSerializedOnTheWire() {
        Citation citation = new Citation("ev-9", "doc-9", "c9", "hop=2", "q-9",
            "ADAPTIVE_HYBRID", "e-17", "doc-9", "https://example.test/doc-9",
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
        TextChunk first = chunk("c1", "d1", "one", 2);
        TextChunk second = chunk("c2", "d1", "two", 2);
        Evidence evidence = textEvidence("e1", "one two", first, second);
        AnswerResponse response = consumer().synthesize(new AnswerRequest("q", 8, "req-3",
            "gv", "iv", "graph", "FIXED_HYBRID"), Collections.singletonList(evidence));
        List<Citation> citations = response.getCitations();
        assertEquals(2, citations.size());
        for (Citation citation : citations) {
            assertNotNull(citation.getQueryId());
            assertEquals("FIXED_HYBRID", citation.getMode());
            assertEquals("d1", citation.getDocumentId());
        }
    }

    private static ExtractiveAnswerConsumer consumer() {
        return new ExtractiveAnswerConsumer(new DeterministicTokenEstimator());
    }

    private static TextChunk chunk(String id, String document, String text, int tokens) {
        return new TextChunk(id, document, 0, 0, text.length(), tokens, text, null, null,
            "https://example.test/" + document, "dataset", "v1", "test", "hash-" + document);
    }

    private static Evidence textEvidence(String id, String text, TextChunk... chunks) {
        java.util.List<SourceRef> sources = new java.util.ArrayList<>();
        for (TextChunk chunk : chunks) {
            if (sources.stream().noneMatch(source -> source.getDocumentId()
                .equals(chunk.getDocumentId()))) {
                sources.add(new SourceRef(chunk.getDocumentId(), chunk.getSourceUri(), null, null,
                    "dataset", "v1", "test", "hash-" + chunk.getDocumentId()));
            }
        }
        return new Evidence(id, EvidenceKind.CHUNK, text, Arrays.asList(chunks),
            Collections.emptyList(), sources, Collections.emptyMap(), null, null, 1,
            Collections.emptyList(), Evidence.CURRENT_SCHEMA_VERSION, "dataset", "v1", "test",
            null, "iv");
    }
}
