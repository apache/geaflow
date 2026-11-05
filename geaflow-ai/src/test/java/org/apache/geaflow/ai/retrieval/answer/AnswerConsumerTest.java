/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.junit.jupiter.api.Test;

class AnswerConsumerTest {
    @Test
    void ordersByScoreAndCitesChunk() {
        Evidence evidence = chunkEvidence("e1", "Paris", 1,
            new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris"));
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 2),
            Collections.singletonList(evidence));
        assertEquals("e1", response.getCitations().get(0).getEvidenceId());
        assertEquals("c1", response.getCitations().get(0).getChunkId());
        assertEquals(2, response.getTokens());
    }

    @Test
    void reportsCitationCoverageOverTheOfferedEvidence() {
        Evidence first = chunkEvidence("e1", "Paris", 2,
            new TextChunk("c1", "d1", 0, 0, 5, 4, "Paris"));
        Evidence second = chunkEvidence("e2", "Lyon", 1,
            new TextChunk("c2", "d2", 0, 0, 5, 4, "Lyon"));
        AnswerResponse partial = consumer().synthesize(new AnswerRequest("where", 4),
            Arrays.asList(first, second));
        assertEquals(2, partial.getConsideredEvidenceCount());
        assertEquals(1, partial.getEvidenceIds().size());
        assertEquals(0.5D, partial.getCitationCoverage(), 1.0e-12);
        assertEquals(0.0D, consumer().synthesize(new AnswerRequest("where", 1),
            Arrays.asList(first, second)).getCitationCoverage(), 1.0e-12);
        assertEquals(0.0D, consumer().synthesize(new AnswerRequest("where", 10),
            Collections.emptyList()).getCitationCoverage(), 1.0e-12);
    }

    @Test
    void abstainsWhenBudgetCannotFitEvidence() {
        Evidence evidence = chunkEvidence("e1", "Paris", 1,
            new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris"));
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 1),
            Collections.singletonList(evidence));
        assertTrue(response.isAbstained());
        assertEquals("TOKEN_BUDGET_OR_INVALID_PROVENANCE", response.getAbstentionReason());
    }

    @Test
    void citesEverySelectedChunkAndKeepsScoreOrder() {
        Evidence evidence = chunkEvidence("e1", "Paris France", 1,
            new TextChunk("c1", "d1", 0, 0, 5, 1, "Paris"),
            new TextChunk("c2", "d1", 1, 5, 10, 1, "France"));
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 4),
            Collections.singletonList(evidence));
        assertEquals(2, response.getCitations().size());
        assertEquals("c2", response.getCitations().get(1).getChunkId());
    }

    @Test
    void validatesResponseInvariants() {
        assertThrows(IllegalArgumentException.class,
            () -> new AnswerResponse("answer", null, "context", 1, Collections.emptyList()));
        assertThrows(IllegalArgumentException.class,
            () -> new AnswerResponse(null, "reason", "", -1, Collections.emptyList()));
    }

    @Test
    void citesGraphPathEvidenceWithoutDocumentSource() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 6, 1, "Entity",
            null, null, "file:///d1", "dataset", "v1", "test", "hash-d1");
        SourceRef source = new SourceRef("d1", "file:///d1", null, null,
            "dataset", "v1", "test", "hash-d1");
        GraphVertexRef vertex = new GraphVertexRef("THING", "v1", "e1",
            "dataset", "v1", "test", "hash-d1");
        GraphPathRef path = new GraphPathRef(Collections.singletonList(vertex),
            Collections.emptyList(), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q1",
            org.apache.geaflow.ai.retrieval.api.model.RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0D, "graph-v1", path, Collections.singletonList("c1"));
        EntityRef entity = new EntityRef("e1", "Entity", Collections.emptyList(), "THING",
            Collections.singletonList("c1"), "dataset", "v1", "test", "hash-d1");
        Evidence evidence = new Evidence("path-evidence", EvidenceKind.GRAPH_PATH,
            "entity context", Collections.singletonList(chunk), Collections.singletonList(entity),
            Collections.singletonList(source), Collections.emptyMap(), null, 1.0D, 1,
            Collections.singletonList(trace), Evidence.CURRENT_SCHEMA_VERSION,
            "dataset", "v1", "test", "graph-v1", "index-v1");
        AnswerResponse response = consumer().synthesize(new AnswerRequest("entity?", 10),
            Collections.singletonList(evidence));
        assertEquals("path-evidence", response.getCitations().get(0).getEvidenceId());
        assertEquals("d1", response.getCitations().get(0).getSourceId());
        assertEquals("e1", response.getCitations().get(0).getSeedEntityId());
    }

    @Test
    void chargesAndReturnsChunkTextInsteadOfMismatchedEvidenceText() {
        Evidence evidence = chunkEvidence("e1", "A much longer unrelated payload", 1,
            new TextChunk("c1", "d1", 0, 0, 5, 1, "Paris"));
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 2),
            Collections.singletonList(evidence));
        assertEquals("Paris", response.getContext());
        assertEquals(1, response.getTokens());
    }

    @Test
    void recordsWhyEachEvidenceItemWasSuppressed() {
        Evidence kept = chunkEvidence("e1", "Paris", 2,
            new TextChunk("c1", "d1", 0, 0, 5, 4, "Paris"));
        Evidence overBudget = chunkEvidence("e2", "Lyon", 1,
            new TextChunk("c2", "d2", 0, 0, 40, 8, "A very long Lyon passage indeed"));
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 4),
            Arrays.asList(kept, overBudget));
        assertEquals("TOKEN_BUDGET_EXCEEDED", response.getSuppressReasons().get("e2"));
        assertTrue(!response.getSuppressReasons().containsKey("e1"));
    }

    @Test
    void recordsInvalidProvenanceAsASuppressReason() {
        Evidence broken = new Evidence("e9", EvidenceKind.CHUNK, "text",
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.emptyMap(), null, null, 1, Collections.emptyList(),
            Evidence.CURRENT_SCHEMA_VERSION, null, null, null, null, null);
        AnswerResponse response = consumer().synthesize(new AnswerRequest("where", 10),
            Collections.singletonList(broken));
        assertEquals("TOKEN_BUDGET_OR_INVALID_PROVENANCE", response.getAbstentionReason());
        assertEquals("INVALID_PROVENANCE", response.getSuppressReasons().get("e9"));
    }

    @Test
    void carriesRetrievalVersionsForTraceability() {
        Evidence evidence = chunkEvidence("e1", "Paris", 1,
            new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris"));
        AnswerResponse response = consumer().synthesize(
            new AnswerRequest("where", 4, "req-1", "gv-1", "iv-1"),
            Collections.singletonList(evidence));
        assertEquals("req-1", response.getRequestId());
        assertEquals("gv-1", response.getGraphVersion());
        assertEquals("iv-1", response.getIndexVersion());
    }

    private static ExtractiveAnswerConsumer consumer() {
        return new ExtractiveAnswerConsumer(new DeterministicTokenEstimator());
    }

    private static Evidence chunkEvidence(String id, String text, int rank, TextChunk... raw) {
        List<TextChunk> chunks = new ArrayList<>();
        List<SourceRef> sources = new ArrayList<>();
        for (TextChunk chunk : raw) {
            String uri = "file:///" + chunk.getDocumentId();
            String hash = "hash-" + chunk.getDocumentId();
            chunks.add(new TextChunk(chunk.getChunkId(), chunk.getDocumentId(), chunk.getChunkIndex(),
                chunk.getStartOffset(), chunk.getEndOffset(), chunk.getTokenEstimate(), chunk.getText(),
                null, null, uri, "dataset", "v1", "test", hash));
            if (sources.stream().noneMatch(source -> source.getDocumentId()
                .equals(chunk.getDocumentId()))) {
                sources.add(new SourceRef(chunk.getDocumentId(), uri, null, null,
                    "dataset", "v1", "test", hash));
            }
        }
        return new Evidence(id, EvidenceKind.CHUNK, text, chunks, Collections.emptyList(), sources,
            Collections.emptyMap(), null, null, rank, Collections.emptyList(),
            Evidence.CURRENT_SCHEMA_VERSION, "dataset", "v1", "test", null, "index-v1");
    }

    private static GraphVertexRef vertex(String vertexId, String entityId) {
        return new GraphVertexRef("THING", vertexId, entityId);
    }
}
