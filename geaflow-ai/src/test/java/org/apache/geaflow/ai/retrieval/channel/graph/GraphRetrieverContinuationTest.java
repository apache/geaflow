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
package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.junit.jupiter.api.Test;

/**
 * Pins the split-expansion contract that the adaptive path depends on.
 *
 * <p>The adaptive executor stops after the first hop so the routing policy can read the measured
 * growth rate and marginal yield before funding a second hop. That is only sound if expanding hop by
 * hop produces exactly the same evidence, counters and stop reason as one uninterrupted call.
 * These tests assert that equivalence: the split path must not be a cheaper approximation of the
 * single-shot path, it must be the same computation cut at a different point.
 */
class GraphRetrieverContinuationTest {

    @Test
    void splittingByHopProducesTheSameHitsCountersAndStopReason() {
        Fixture fixture = twoHopChain();

        List<GraphRetriever.GraphHit> singleShot = fixture.retriever.retrieve(fixture.query,
            fixture.entities, fixture.provider, fixture.chunks, fixture.config, 0L, System::nanoTime,
            null, Collections.singletonList("c1"));
        GraphRetriever.GraphStats singleStats = fixture.retriever.getLastStats();

        GraphRetriever splitRetriever = new GraphRetriever(fixture.resolver, fixture.tokenEstimator());
        GraphRetriever.ExpansionSession session = splitRetriever.beginRetrieval(fixture.query,
            fixture.entities, fixture.provider, fixture.chunks, fixture.config, 0L, System::nanoTime,
            null, Collections.singletonList("c1"));
        while (!session.isExhausted()) {
            splitRetriever.expandOneHop(session, fixture.config, 0L, System::nanoTime);
        }
        List<GraphRetriever.GraphHit> split = session.finish();

        assertEquals(describe(singleShot), describe(split));
        assertEquals(singleStats.getEdgesExamined(), session.getStats().getEdgesExamined());
        assertEquals(singleStats.getNeighborsSampled(), session.getStats().getNeighborsSampled());
        assertEquals(singleStats.getVerticesReached(), session.getStats().getVerticesReached());
        assertEquals(singleStats.getCandidatesProduced(), session.getStats().getCandidatesProduced());
        assertEquals(singleStats.getEvidenceYield(), session.getStats().getEvidenceYield());
        assertEquals(singleStats.getGrowthRate(), session.getStats().getGrowthRate(), 1e-9);
        assertEquals(singleStats.getMarginalYield(), session.getStats().getMarginalYield(), 1e-9);
        assertEquals(singleStats.getStopReason(), session.getStats().getStopReason());
    }

    @Test
    void oneHopCallDoesNotConsumeTheNextHopsFrontier() {
        // The whole point of the split is that hop 2 is still pending when hop 1 returns. If one call
        // drained the frontier there would be nothing left for the policy to decide about.
        Fixture fixture = twoHopChain();
        GraphRetriever retriever = new GraphRetriever(fixture.resolver, fixture.tokenEstimator());
        GraphRetriever.ExpansionSession session = retriever.beginRetrieval(fixture.query,
            fixture.entities, fixture.provider, fixture.chunks, fixture.config, 0L, System::nanoTime,
            null, Collections.singletonList("c1"));

        assertEquals(0, session.getCurrentHop());

        retriever.expandOneHop(session, fixture.config, 0L, System::nanoTime);

        assertTrue(session.getCurrentHop() >= 1, "hop 1 must have run and left hop 2 pending");
        assertFalse(session.isExhausted(), "a two-hop chain cannot be exhausted after one hop");
        assertTrue(session.getStats().getEdgesExamined() >= 1);
        int afterFirstHop = session.getStats().getEdgesExamined();

        retriever.expandOneHop(session, fixture.config, 0L, System::nanoTime);

        assertTrue(session.getStats().getEdgesExamined() > afterFirstHop,
            "the second hop must scan more edges than the first");
    }

    @Test
    void measuredGrowthAndYieldBecomeAvailableAfterTheFirstHop() {
        // The policy can only stop a runaway expansion if these become non-zero before the second
        // hop is funded. Before the split they were only written after the whole expansion.
        Fixture fixture = twoHopChain();
        GraphRetriever retriever = new GraphRetriever(fixture.resolver, fixture.tokenEstimator());
        GraphRetriever.ExpansionSession session = retriever.beginRetrieval(fixture.query,
            fixture.entities, fixture.provider, fixture.chunks, fixture.config, 0L, System::nanoTime,
            null, Collections.singletonList("c1"));

        assertEquals(0.0D, session.getStats().getGrowthRate());

        retriever.expandOneHop(session, fixture.config, 0L, System::nanoTime);

        assertTrue(session.getStats().getGrowthRate() > 0.0D,
            "growth rate must be measured once a hop has produced new vertices");
        assertTrue(session.getStats().getMarginalYield() > 0.0D,
            "marginal yield must be measured once a hop has produced evidence");
    }

    @Test
    void seedingAloneExpandsNothingAndReportsTheAnchor() {
        Fixture fixture = twoHopChain();
        GraphRetriever retriever = new GraphRetriever(fixture.resolver, fixture.tokenEstimator());

        GraphRetriever.ExpansionSession session = retriever.beginRetrieval(fixture.query,
            fixture.entities, fixture.provider, fixture.chunks, fixture.config, 0L, System::nanoTime,
            null, Collections.singletonList("c1"));

        assertFalse(session.isTerminal());
        assertEquals(1, session.getStats().getAnchorsConsidered());
        assertEquals(0, session.getStats().getEdgesExamined());
        assertTrue(session.getHits().isEmpty());
    }

    @Test
    void lowMarginalYieldStopsTheRemainingFrontierStates() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Virtue", "topic"),
            new EntityRef("e4", "Justice", "topic"));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 6, 1, "Virtue"));
        chunks.put("c3", new TextChunk("c3", "doc-3", 0, 0, 7, 1, "Justice"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "supports", "e1", "e3", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e4", Collections.singletonList("c3")));
        GraphNeighborProvider provider = new InMemoryGraphNeighborProvider(edges);
        GraphRetriever retriever = new GraphRetriever(new EntityAnchorResolver(entities));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder().maxHop(2).candidateBudget(10)
            .edgeScanBudget(10).neighborCap(10).minMarginalYield(1.1D).build();
        GraphRetriever.ExpansionSession session = retriever.beginRetrieval("Confucius Ethics", entities,
            provider, chunks, config, 0L, System::nanoTime, null, Collections.emptyList());

        retriever.expandOneHop(session, config, 0L, System::nanoTime);

        assertTrue(session.isStopped());
        assertEquals(RecallStopReason.MARGINAL_YIELD_LIMIT, session.getStats().getStopReason());
        assertEquals(1, session.getStats().getEdgesExamined());
        assertEquals(1, session.getStats().getCandidatesProduced());
    }

    private static List<String> describe(List<GraphRetriever.GraphHit> hits) {
        List<String> described = new ArrayList<>();
        for (GraphRetriever.GraphHit hit : hits) {
            described.add(hit.getPath().getVertexIds() + "|" + hit.getPath().getEdgeIds() + "|"
                + hit.getSourceChunkIds());
        }
        return described;
    }

    /** e1 --edge1--> e2 --edge2--> e3, each edge contributing its own source chunk. */
    private static Fixture twoHopChain() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Virtue", "topic"));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 8, 2, "Confucius"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 6, 1, "Ethics"));
        chunks.put("c3", new TextChunk("c3", "doc-3", 0, 0, 6, 1, "Virtue"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "supports", "e1", "e2", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e3", Collections.singletonList("c3")));

        Fixture fixture = new Fixture();
        fixture.entities = entities;
        fixture.chunks = chunks;
        fixture.provider = new InMemoryGraphNeighborProvider(edges);
        fixture.resolver = new EntityAnchorResolver(entities);
        fixture.retriever = new GraphRetriever(fixture.resolver);
        fixture.query = "Confucius";
        fixture.config = GraphRetrievalConfig.builder().maxHop(2).candidateBudget(10)
            .edgeScanBudget(10).neighborCap(10).minMarginalYield(0.0D).build();
        return fixture;
    }

    private static final class Fixture {
        private String query;
        private List<EntityRef> entities;
        private Map<String, TextChunk> chunks;
        private GraphNeighborProvider provider;
        private EntityAnchorResolver resolver;
        private GraphRetriever retriever;
        private GraphRetrievalConfig config;

        private org.apache.geaflow.ai.retrieval.execution.TokenEstimator tokenEstimator() {
            return new org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator();
        }
    }
}
