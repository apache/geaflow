/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.junit.jupiter.api.Test;

class AnswerConsumerTest {
    @Test
    void ordersByScoreAndCitesChunk() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(chunk), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 1);
        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 2), Collections.singletonList(evidence));
        assertEquals("e1", response.getCitations().get(0).getEvidenceId());
        assertEquals("c1", response.getCitations().get(0).getChunkId());
        assertEquals(2, response.getTokens());
    }

    @Test
    void reportsCitationCoverageOverTheOfferedEvidence() {
        TextChunk first = new TextChunk("c1", "d1", 0, 0, 5, 4, "Paris");
        TextChunk second = new TextChunk("c2", "d2", 0, 0, 5, 4, "Lyon");
        Evidence firstEvidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(first), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 2);
        Evidence secondEvidence = new Evidence("e2", EvidenceKind.CHUNK, "Lyon",
            Collections.singletonList(second), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d2", "file:///d2")), new HashMap<>(), null, 1);
        ExtractiveAnswerConsumer consumer =
            new ExtractiveAnswerConsumer(new DeterministicTokenEstimator());

        // Only the highest-scored evidence fits the budget: 1 of 2 offered items is cited.
        AnswerResponse partial = consumer.consume(new AnswerRequest("where", 4),
            java.util.Arrays.asList(firstEvidence, secondEvidence));
        assertEquals(2, partial.getConsideredEvidenceCount());
        assertEquals(1, partial.getEvidenceIds().size());
        assertEquals(0.5D, partial.getCitationCoverage(), 1.0e-12);

        // An abstained response never reports NaN.
        AnswerResponse abstained = consumer.consume(new AnswerRequest("where", 1),
            java.util.Arrays.asList(firstEvidence, secondEvidence));
        assertEquals(0.0D, abstained.getCitationCoverage(), 1.0e-12);

        // No evidence at all also reports 0.0 rather than dividing by zero.
        assertEquals(0.0D, consumer.consume(new AnswerRequest("where", 10),
            Collections.emptyList()).getCitationCoverage(), 1.0e-12);
    }

    @Test
    void abstainsWhenBudgetCannotFitEvidence() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(chunk), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 1);
        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 1), Collections.singletonList(evidence));
        assertTrue(response.isAbstained());
        assertEquals("TOKEN_BUDGET_OR_INVALID_PROVENANCE", response.getAbstentionReason());
    }

    @Test
    void citesEverySelectedChunkAndKeepsScoreOrder() {
        TextChunk first = new TextChunk("c1", "d1", 0, 0, 5, 1, "Paris");
        TextChunk second = new TextChunk("c2", "d1", 1, 5, 10, 1, "France");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris France",
            Arrays.asList(first, second), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 1);
        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 4), Collections.singletonList(evidence));
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
        GraphPathRef path = new GraphPathRef(Collections.singletonList("v1"),
            Collections.emptyList(), 0, false);
        Evidence evidence = new Evidence("path-evidence", EvidenceKind.GRAPH_PATH, "entity context",
            Collections.emptyList(), Collections.emptyList(), Collections.singletonList(path),
            Collections.emptyList(), new HashMap<>(), null, 1);
        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("entity?", 10), Collections.singletonList(evidence));
        assertEquals("path-evidence", response.getCitations().get(0).getEvidenceId());
        assertEquals("graph", response.getCitations().get(0).getSourceId());
        assertTrue(response.getCitations().get(0).getPathId().contains("v1"));
    }

    @Test
    void chargesAndReturnsChunkTextInsteadOfMismatchedEvidenceText() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 5, 1, "Paris");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "A much longer unrelated payload",
            Collections.singletonList(chunk), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 1);
        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 2), Collections.singletonList(evidence));
        assertEquals("Paris", response.getContext());
        assertEquals(1, response.getTokens());
    }

    @Test
    void recordsWhyEachEvidenceItemWasSuppressed() {
        TextChunk first = new TextChunk("c1", "d1", 0, 0, 5, 4, "Paris");
        TextChunk tooLarge = new TextChunk("c2", "d2", 0, 0, 40, 8, "A very long Lyon passage indeed");
        Evidence kept = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(first), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 2);
        Evidence overBudget = new Evidence("e2", EvidenceKind.CHUNK, "Lyon",
            Collections.singletonList(tooLarge), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d2", "file:///d2")), new HashMap<>(), null, 1);

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 4), Arrays.asList(kept, overBudget));

        assertEquals("TOKEN_BUDGET_EXCEEDED", response.getSuppressReasons().get("e2"),
            "each suppressed item needs an explicit reason: " + response.getSuppressReasons());
        assertTrue(!response.getSuppressReasons().containsKey("e1"));
    }

    @Test
    void recordsInvalidProvenanceAsASuppressReason() {
        Evidence broken = new Evidence("e9", EvidenceKind.CHUNK, "text",
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList(), new HashMap<>(), null, 1);

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 10), Collections.singletonList(broken));

        assertEquals("TOKEN_BUDGET_OR_INVALID_PROVENANCE", response.getAbstentionReason());
        assertEquals("INVALID_PROVENANCE", response.getSuppressReasons().get("e9"),
            "an unusable evidence item must be reported: " + response.getSuppressReasons());
        assertTrue(response.getSuppressReasons().containsKey("e9"),
            "an unusable evidence item must be reported: " + response.getSuppressReasons());
    }

    @Test
    void carriesRetrievalVersionsForTraceability() {
        TextChunk chunk = new TextChunk("c1", "d1", 0, 0, 5, 2, "Paris");
        Evidence evidence = new Evidence("e1", EvidenceKind.CHUNK, "Paris",
            Collections.singletonList(chunk), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new SourceRef("d1", "file:///d1")), new HashMap<>(), null, 1);

        AnswerResponse response = new ExtractiveAnswerConsumer(new DeterministicTokenEstimator())
            .consume(new AnswerRequest("where", 4, "req-1", "gv-1", "iv-1"),
                Collections.singletonList(evidence));

        assertEquals("req-1", response.getRequestId());
        assertEquals("gv-1", response.getGraphVersion());
        assertEquals("iv-1", response.getIndexVersion());
    }
}
