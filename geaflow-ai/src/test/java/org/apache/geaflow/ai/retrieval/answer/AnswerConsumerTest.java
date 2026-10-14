/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
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
}
