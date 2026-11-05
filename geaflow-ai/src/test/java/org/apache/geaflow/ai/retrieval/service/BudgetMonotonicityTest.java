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
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.junit.jupiter.api.Test;

/**
 * Pins that the three primary constraints degrade monotonically.
 *
 * <p>The design allows one hard primary constraint per run - latency, candidate count, or context
 * tokens - and requires all modes to be comparable under them. Each constraint was exercised in
 * isolation, but nothing asserted the property that makes the comparison meaningful: relaxing a
 * budget must never lose evidence that a tighter budget already returned. A budget that behaves
 * non-monotonically cannot be used for equal-budget comparison, because a method could appear
 * better simply by being given a stricter setting.
 *
 * <p>Each test drives the same query through the same fixture at increasing budget levels and
 * asserts the evidence set only grows.
 */
class BudgetMonotonicityTest {
    private static final String GRAPH = "monotonic-graph";
    private static final String VERSION = "v1";
    private static final String QUERY = "Ada Lovelace";

    @Test
    void candidateBudgetNeverLosesEvidenceItAlreadyReturned() {
        List<Set<String>> levels = new ArrayList<>();
        for (int candidates : new int[] {5, 10, 20, 40}) {
            levels.add(evidenceIds(retrieve(BudgetConstraint.CANDIDATES,
                new RetrievalBudget(5, 5000, candidates, 16000, BudgetConstraint.CANDIDATES))));
        }
        assertMonotonic("candidates", levels);
    }

    @Test
    void tokenBudgetNeverLosesEvidenceItAlreadyReturned() {
        List<Set<String>> levels = new ArrayList<>();
        for (int tokens : new int[] {64, 128, 256, 512}) {
            levels.add(evidenceIds(retrieve(BudgetConstraint.TOKENS,
                new RetrievalBudget(5, 5000, 100, tokens, BudgetConstraint.TOKENS))));
        }
        assertMonotonic("tokens", levels);
    }

    @Test
    void latencyBudgetNeverLosesEvidenceItAlreadyReturned() {
        List<Set<String>> levels = new ArrayList<>();
        for (int timeoutMs : new int[] {500, 1000, 2000, 4000}) {
            levels.add(evidenceIds(retrieve(BudgetConstraint.LATENCY,
                new RetrievalBudget(5, timeoutMs, 100, 16000, BudgetConstraint.LATENCY))));
        }
        assertMonotonic("latency", levels);
    }

    @Test
    void tighterBudgetNeverReturnsMoreEvidenceThanALooserOne() {
        // The converse direction: a larger budget may return more, but never fewer.
        int small = evidenceIds(retrieve(BudgetConstraint.CANDIDATES,
            new RetrievalBudget(5, 5000, 5, 16000, BudgetConstraint.CANDIDATES))).size();
        int large = evidenceIds(retrieve(BudgetConstraint.CANDIDATES,
            new RetrievalBudget(5, 5000, 40, 16000, BudgetConstraint.CANDIDATES))).size();
        assertTrue(large >= small,
            "a 40-candidate budget returned " + large + " items, fewer than the 5-candidate budget's "
                + small);
    }

    private static void assertMonotonic(String constraint, List<Set<String>> levels) {
        assertNotNull(levels);
        // A vacuous monotonicity check (every level empty) would pass while proving nothing, so the
        // loosest level must actually return evidence.
        Set<String> widest = levels.get(levels.size() - 1);
        assertTrue(!widest.isEmpty(),
            constraint + " monotonicity proved nothing: the loosest budget returned no evidence");
        for (int i = 1; i < levels.size(); i++) {
            Set<String> previous = levels.get(i - 1);
            Set<String> current = levels.get(i);
            Set<String> lost = new HashSet<>(previous);
            lost.removeAll(current);
            assertTrue(lost.isEmpty(),
                constraint + " budget level " + i + " lost evidence the previous level returned: "
                    + lost);
        }
    }

    private static Set<String> evidenceIds(RetrievalResponse response) {
        Set<String> ids = new HashSet<>();
        for (Evidence evidence : response.getEvidence()) {
            ids.add(evidence.getEvidenceId());
        }
        return ids;
    }

    private static RetrievalResponse retrieve(BudgetConstraint constraint, RetrievalBudget budget) {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        List<TextChunk> chunks = chunks();
        List<EntityRef> entities = entities();
        List<GraphEdgeRef> edges = edges();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks, entities, edges, true);

        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(VERSION);
        request.setQuery(QUERY);
        request.setMode("GRAPH_ONLY");
        request.setBudget(budget);
        assertEquals(constraint, budget.getPrimaryConstraint());
        return new RecallService(registry).retrieve(request);
    }

    private static List<TextChunk> chunks() {
        List<TextChunk> chunks = new ArrayList<>();
        chunks.add(chunk("chunk-1", "Ada Lovelace worked with Charles Babbage."));
        chunks.add(chunk("chunk-2", "Charles Babbage designed the Analytical Engine."));
        chunks.add(chunk("chunk-3", "The Analytical Engine was never completed."));
        chunks.add(chunk("chunk-4", "Ada Lovelace wrote the first published algorithm."));
        return chunks;
    }

    private static TextChunk chunk(String id, String text) {
        return new TextChunk(id, "doc-" + id, 0, 0, text.length(), Math.max(1, text.length() / 4), text,
            "chunk-v1", null, "https://example.test/doc-" + id);
    }

    private static List<EntityRef> entities() {
        return Arrays.asList(
            new EntityRef("e-ada", "Ada Lovelace", Collections.emptyList(), "person",
                Collections.singletonList("chunk-1")),
            new EntityRef("e-babbage", "Charles Babbage", Collections.emptyList(), "person",
                Collections.singletonList("chunk-2")),
            new EntityRef("e-engine", "Analytical Engine", Collections.emptyList(), "device",
                Collections.singletonList("chunk-3")));
    }

    private static List<GraphEdgeRef> edges() {
        return Arrays.asList(
            new GraphEdgeRef("rel-1", "mentions", "e-ada", "e-babbage",
                Collections.singletonList("chunk-1")),
            new GraphEdgeRef("rel-2", "designed", "e-babbage", "e-engine",
                Collections.singletonList("chunk-2")),
            new GraphEdgeRef("rel-3", "wrote", "e-ada", "e-engine",
                Collections.singletonList("chunk-4")));
    }
}