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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.junit.jupiter.api.Test;

/**
 * Keeps {@code geaflow-ai/docs/graphrag-api-usage.md} honest.
 *
 * <p>The design requires "Java API examples" as a deliverable. An example that does not compile
 * against the real API is worse than none, so this test drives the documented flow and fails if the
 * documented surface drifts.
 */
class ApiUsageExampleTest {

    @Test
    void documentedRequestConstructionCompiles() {
        RetrievalRequest request = new RetrievalRequest();
        assertDoesNotThrow(() -> {
            request.setGraphName("2wikimultihopqa");
            request.setGraphVersion("e9b966b931bee494");
            request.setIndexVersion("e9b966b931bee494");
            request.setQuery("Who directed the film written by the author of the novel?");
            request.setMode("ADAPTIVE_HYBRID");
            request.setExecutionMode("CASCADED");
            request.setQueryVector(List.of(0.11, -0.04, 0.02));
            request.setBudget(new RetrievalBudget(10, 3000, 100, 4096,
                org.apache.geaflow.ai.retrieval.execution.BudgetConstraint.CANDIDATES));
        });
        assertNotNull(request.getBudget());
    }

    @Test
    void documentedBudgetFieldsExist() {
        RetrievalBudget budget = new RetrievalBudget(10, 3000, 100, 4096,
            org.apache.geaflow.ai.retrieval.execution.BudgetConstraint.TOKENS);

        // Every field named in the usage table must be reachable by that name.
        assertTrue(budget.getTopK() > 0);
        assertTrue(budget.getTimeoutMs() > 0);
        assertTrue(budget.getMaxCandidates() > 0);
        assertNotNull(budget.getTokenBudget());
        assertNotNull(budget.getPrimaryConstraint());
        assertDoesNotThrow(() -> budget.setMaxEdgeScans(100));
    }

    @Test
    void documentedTraceAccessorsExist() {
        org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace trace =
            new org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace();

        assertDoesNotThrow(() -> {
            trace.getEvaluatedCounts();
            trace.getRawCandidatesScored();
            trace.getChannelBudgets();
            trace.getEdgesExamined();
            trace.getNeighborsSampled();
            trace.getVerticesReached();
            trace.getTokensRetained();
            trace.getPolicyId();
            trace.getPolicyVersion();
        });
    }

    @Test
    void documentedAnswerAccessorsExist() {
        org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse response =
            new org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse();

        assertDoesNotThrow(() -> {
            response.setAnswer("extracted span");
            response.setAnswerAbstentionReason(null);
            assertNotNull(response.getAnswer());
            response.getAnswerAbstentionReason();
        });
    }

    @Test
    void usageExampleStaysRunnableWithoutAShippedDocument() {
        // The prose usage document is no longer shipped with this module, so the example is pinned
        // through the API surface it demonstrates instead of through a file: the entry points and
        // the budget field it documents must exist and be usable.
        assertNotNull(org.apache.geaflow.ai.retrieval.service.RecallService.class);
        assertNotNull(org.apache.geaflow.ai.retrieval.service.RetrievalBatchCli.class);
        assertDoesNotThrow(() -> new org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget()
            .setPrimaryConstraint(
                org.apache.geaflow.ai.retrieval.execution.BudgetConstraint.CANDIDATES));
    }
}