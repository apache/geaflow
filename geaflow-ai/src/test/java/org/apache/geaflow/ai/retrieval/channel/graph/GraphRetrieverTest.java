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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.junit.jupiter.api.Test;

class GraphRetrieverTest {

    @Test
    void defaultPrioritizedNeighborsRanksWithinTheScanWindow() {
        GraphNeighborProvider provider = entityId -> Arrays.asList(edge(0), edge(1), edge(2));

        List<GraphEdgeRef> neighbors = provider.prioritizedNeighbors("e1",
            java.util.Comparator.comparing(GraphEdgeRef::getEdgeId).reversed(), 2);

        assertEquals(2, neighbors.size());
        assertEquals("edge-000001", neighbors.get(0).getEdgeId());
        assertEquals("edge-000000", neighbors.get(1).getEdgeId());
    }

    @Test
    void edgeCapStopsNeighborIterationWithoutCopyingOrSortingTheWholeList() {
        AtomicInteger reads = new AtomicInteger();
        List<GraphEdgeRef> neighbors = new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                if (index >= 2) {
                    throw new AssertionError("retriever read beyond edge cap");
                }
                return edge(index);
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphNeighborProvider provider = entityId -> neighbors;
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities, provider,
            chunks, 10, 2, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        assertEquals(2, reads.get());
        assertEquals(2, retriever.getLastStats().getEdgesExamined());
    }

    @Test
    void expiredDeadlineDoesNotReadNeighbors() {
        AtomicInteger reads = new AtomicInteger();
        GraphNeighborProvider provider = entityId -> new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                return edge(index);
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphRetriever retriever = new GraphRetriever();
        List<EntityRef> entities = Collections.singletonList(new EntityRef("e1", "Confucius", "person"));

        retriever.retrieve("Confucius", entities, provider, Collections.emptyMap(),
            10, 10, 1L, () -> 1L);

        assertEquals(0, reads.get());
    }

    @Test
    void expandsFromTextCandidateAnchorWhenQuestionHasNoEntityMention() {
        EntityRef first = new EntityRef("e1", "Confucius", Collections.emptyList(), "person",
            Collections.singletonList("c1"));
        EntityRef second = new EntityRef("e2", "Ethics", Collections.emptyList(), "topic",
            Collections.singletonList("c2"));
        List<EntityRef> entities = Arrays.asList(first, second);
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "supports", "e1", "e2",
            Collections.singletonList("c2"));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 8, 2, "candidate"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 6, 1, "evidence"));
        GraphRetriever retriever = new GraphRetriever(new EntityAnchorResolver(entities));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder().maxHop(1).candidateBudget(4)
            .edgeScanBudget(4).neighborCap(4).build();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("unmatched question", entities,
            new InMemoryGraphNeighborProvider(Collections.singletonList(edge)), chunks, config,
            0L, System::nanoTime, null, Collections.singletonList("c1"));

        assertEquals(1, hits.size());
        assertEquals("TEXT_CANDIDATE", hits.get(0).getAnchor().getMatchType());
        assertEquals(1, retriever.getLastStats().getAnchorsConsidered());
    }

    @Test
    void duplicateProvenanceScanStopsAtCandidateBudget() {
        List<String> provenance = new java.util.ArrayList<>();
        for (int index = 0; index < 1000; index++) {
            provenance.add("c1");
        }
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "supports", "e1", "e2", provenance);
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            Collections.singletonList(edge), chunks, 4, 10, 0L);

        assertEquals(1, hits.size());
        assertEquals(1, retriever.getLastStats().getCandidatesProduced());
        assertEquals(RecallStopReason.CANDIDATE_LIMIT, retriever.getLastStats().getStopReason());
    }

    @Test
    void preservesDistinctGraphVertexIdentityInPathAndSeed() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        GraphNeighborProvider provider = new GraphNeighborProvider() {
            @Override
            public List<GraphEdgeRef> neighbors(String entityId) {
                return Collections.singletonList(edge(0));
            }

            @Override
            public GraphVertexRef vertex(EntityRef entity) {
                return new GraphVertexRef(entity.getType(), "vertex-" + entity.getEntityId(),
                    entity.getEntityId());
            }
        };
        Map<String, TextChunk> chunks = Collections.singletonMap("c1",
            new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities, provider,
            chunks, 10, 10, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals("e1", hits.get(0).getSeed().getEntityId());
        assertEquals("vertex-e1", hits.get(0).getSeed().getVertexId());
        assertEquals(Arrays.asList("vertex-e1", "vertex-e2"),
            hits.get(0).getPath().getVertexIds());
        assertEquals(hits.get(0).getSeed().getVertexId(),
            hits.get(0).getPath().getVertexIds().get(0));
        assertTrue(new GraphEvidenceTrace("request-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v1", hits.get(0).getPath(),
            hits.get(0).getSourceChunkIds()).getSeed().equals(hits.get(0).getSeed()));
    }

    @Test
    void expandsAtMostTwoHopsWithStablePathsAndStats() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "teaches", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(2).candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .samplingSeed(7L).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> first = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);
        GraphRetriever.GraphStats firstStats = retriever.getLastStats();
        List<GraphRetriever.GraphHit> second = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(2, first.size());
        assertEquals(Arrays.asList(1, 2), Arrays.asList(
            first.get(0).getPath().getHop(), first.get(1).getPath().getHop()));
        assertEquals(Collections.singletonList("c1"), first.get(0).getSourceChunkIds());
        assertEquals(Arrays.asList("c1", "c2"), first.get(1).getSourceChunkIds());
        assertEquals(first.get(0).getPath(), second.get(0).getPath());
        assertEquals(first.get(1).getPath(), second.get(1).getPath());
        assertEquals(3, firstStats.getUniqueVertices());
        assertEquals(3, firstStats.getEdgesExamined());
        assertEquals(RecallStopReason.MAX_HOP_REACHED, firstStats.getStopReason());
    }

    @Test
    void lowAssociationFloorTerminatesHopInsteadOfScanningEveryEdge() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Rites", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "teaches", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "teaches", "e1", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        // Association scores grow with hop decay and edge strength; a floor high enough to reject
        // every edge plus a survival rate below 1.0 must abandon the hop early.
        GraphRetrievalConfig terminating = GraphRetrievalConfig.builder().maxHop(1)
            .candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .minAssociationScore(10.0D).minAssociationSurvivalRate(0.5D).build();
        GraphRetriever retriever = new GraphRetriever();
        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, terminating, 0L, System::nanoTime);

        assertTrue(hits.isEmpty());
        GraphRetriever.GraphStats stats = retriever.getLastStats();
        assertEquals(RecallStopReason.LOW_ASSOCIATION_SCORE, stats.getStopReason());
        assertEquals(1, stats.getTerminatedHop());
        // Both edges are examined lazily but only one is scored before the hop is abandoned.
        assertEquals(1, stats.getFilteredByAssociation());
        assertEquals(1, stats.getTerminatedHop());

        // The default survival rate of 1.0 keeps the historical skip-one-edge behaviour.
        GraphRetrievalConfig historical = GraphRetrievalConfig.builder().maxHop(1)
            .candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .minAssociationScore(10.0D).build();
        GraphRetriever baseline = new GraphRetriever();
        baseline.retrieve("Confucius", entities, new InMemoryGraphNeighborProvider(edges),
            chunks, historical, 0L, System::nanoTime);
        GraphRetriever.GraphStats baselineStats = baseline.getLastStats();
        assertNotEquals(RecallStopReason.LOW_ASSOCIATION_SCORE, baselineStats.getStopReason());
        assertEquals(2, baselineStats.getFilteredByAssociation());
        assertEquals(-1, baselineStats.getTerminatedHop());
    }

    @Test
    void maxHopOnePreservesMvpFrontierAndLazyNeighborCap() {
        AtomicInteger reads = new AtomicInteger();
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        List<GraphEdgeRef> neighbors = new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                if (index >= 2) {
                    throw new AssertionError("neighbor cap was not enforced");
                }
                return new GraphEdgeRef("edge-" + index, "supports", "e1", "e2",
                    Collections.singletonList("c1"));
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphNeighborProvider provider = entityId -> neighbors;
        Map<String, TextChunk> chunks = Collections.singletonMap("c1",
            new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(10).edgeScanBudget(10).neighborCap(2).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            provider, chunks, config, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        assertEquals(2, reads.get());
        assertEquals(RecallStopReason.MAX_HOP_REACHED, retriever.getLastStats().getStopReason());
    }

    @Test
    void relationPrioritySelectsStrongEdgeBeyondNeighborCapWithinScanBudget() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Weak", "topic"),
            new EntityRef("e3", "Strong", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "weak", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "strong", "e1", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "weak"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "strong"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder().maxHop(1).neighborCap(1)
            .candidateBudget(10).edgeScanBudget(2).relationPriority("strong", 10.0)
            .relationPriority("weak", 0.1).build();

        GraphRetriever retriever = new GraphRetriever();
        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(Collections.singletonList("edge-2"),
            hits.get(0).getPath().getEdgeIds());
        assertEquals(2, retriever.getLastStats().getEdgesExamined());
    }

    @Test
    void boundedPriorityScanKeepsEvidenceBeforeReportingTruncation() {
        AtomicInteger reads = new AtomicInteger();
        List<GraphEdgeRef> neighbors = new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                reads.incrementAndGet();
                return edge(index);
            }

            @Override
            public int size() {
                return 100_000;
            }
        };
        GraphNeighborProvider provider = new GraphNeighborProvider() {
            @Override
            public List<GraphEdgeRef> neighbors(String entityId) {
                return neighbors;
            }

            @Override
            public NeighborBatch prioritizedNeighbors(String entityId,
                                                       java.util.Comparator<GraphEdgeRef> priority,
                                                       int candidateLimit, int scanLimit) {
                return NeighborBatch.select(neighbors, priority, candidateLimit, scanLimit);
            }
        };
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        Map<String, TextChunk> chunks = Collections.singletonMap("c1",
            new TextChunk("c1", "doc-1", 0, 0, 1, 1, "Confucius"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder().maxHop(1).neighborCap(1)
            .candidateBudget(10).edgeScanBudget(2).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities, provider,
            chunks, config, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals(2, reads.get());
        assertEquals(2, retriever.getLastStats().getEdgesExamined());
        assertEquals(RecallStopReason.EDGE_SCAN_LIMIT, retriever.getLastStats().getStopReason());
    }

    @Test
    void explicitConfigDeadlineDoesNotReadNeighborsAfterAnchorResolution() {
        AtomicInteger reads = new AtomicInteger();
        GraphNeighborProvider provider = entityId -> {
            reads.incrementAndGet();
            return Collections.singletonList(edge(0));
        };
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(10).edgeScanBudget(10).neighborCap(10).build();
        GraphRetriever retriever = new GraphRetriever();
        List<EntityRef> entities = Collections.singletonList(new EntityRef("e1", "Confucius", "person"));

        retriever.retrieve("Confucius", entities, provider, Collections.emptyMap(), config,
            1L, () -> 1L);

        assertEquals(0, reads.get());
        assertEquals(RecallStopReason.DEADLINE, retriever.getLastStats().getStopReason());
    }

    @Test
    void tokenBudgetPreservesCompletedEvidenceBeforeStopping() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "teaches", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 3, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(2).candidateBudget(10).edgeScanBudget(10).tokenBudget(2).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals(RecallStopReason.TOKEN_BUDGET, retriever.getLastStats().getStopReason());
        assertEquals(1, retriever.getLastStats().getEvidenceYield());
    }

    @Test
    void relationPriorityRanksAllowedNeighborsBeforeExpansion() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("a-low", "background", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("z-high", "supports", "e1", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(1).edgeScanBudget(2).neighborCap(2)
            .relationPriority("supports", 2.0).relationPriority("background", 0.1).build();

        List<GraphRetriever.GraphHit> hits = new GraphRetriever().retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals("z-high", hits.get(0).getPath().getEdgeIds().get(0));
    }

    @Test
    void appliesHopDecayOnlyAfterTheFirstHop() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "teaches", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(2).candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .hopDecay(0.5).degreePenalty(0.0).relationPriority("teaches", 2.0)
            .relationPriority("supports", 3.0).build();

        List<GraphRetriever.GraphHit> hits = new GraphRetriever().retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        // Hits are returned in descending association score, and the second hop carries the higher
        // relation priority (supports=3.0) with no decay applied to it, so it ranks first. The hop
        // that decays is the one entered from a scored predecessor, which this fixture never does.
        assertEquals(3.0, hits.get(0).getScore(), 1e-9);
        assertEquals(2.0, hits.get(1).getScore(), 1e-9);
    }

    @Test
    void candidateBudgetKeepsValidationStopReasonWhenMalformedEdgeWasSeen() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-bad", "bad", "e1", "missing", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-good", "supports", "e1", "e2", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(1).edgeScanBudget(2).neighborCap(2)
            .relationPriority("bad", 2.0).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals(RecallStopReason.TRACE_VALIDATION_ERROR, retriever.getLastStats().getStopReason());
        assertTrue(retriever.getLastStats().getValidationErrors().get(0).contains("edge-bad"));
    }

    @Test
    void stopsWhenMarginalYieldFallsEvenIfCurrentEdgeAddsEvidence() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"),
            new EntityRef("e4", "Knowledge", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "supports", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e1", "e3", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-3", "supports", "e1", "e4", Collections.singletonList("c3")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        chunks.put("c3", new TextChunk("c3", "doc-3", 0, 0, 1, 1, "three"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(10).edgeScanBudget(3).neighborCap(3)
            .minMarginalYield(0.75).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(1, hits.size());
        assertEquals(RecallStopReason.MARGINAL_YIELD_LIMIT, retriever.getLastStats().getStopReason());
        assertEquals(1.0 / 3.0, retriever.getLastStats().getMarginalYield(), 1e-9);
    }

    @Test
    void stopsWhenAggregateGrowthExceedsLimitWithinOneHop() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "supports", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e1", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(10).edgeScanBudget(2).neighborCap(2)
            .maxDiffusionGrowth(1.5).build();
        GraphRetriever retriever = new GraphRetriever();

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        assertEquals(RecallStopReason.DIFFUSION_GROWTH_LIMIT, retriever.getLastStats().getStopReason());
        assertEquals(2.0, retriever.getLastStats().getGrowthRate(), 1e-9);
    }

    @Test
    void growthLimitPreservesAllDirectAnchorEvidence() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("a1", "Confucius", "person"),
            new EntityRef("a2", "Plato", "person"),
            new EntityRef("e1", "Ethics", "topic"),
            new EntityRef("e2", "Wisdom", "topic"),
            new EntityRef("e3", "Ideas", "topic"),
            new EntityRef("e4", "Forms", "topic"),
            new EntityRef("e5", "Knowledge", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "supports", "a1", "e1", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "a1", "e2", Collections.singletonList("c2")),
            new GraphEdgeRef("edge-3", "supports", "a2", "e3", Collections.singletonList("c3")),
            new GraphEdgeRef("edge-4", "supports", "a2", "e4", Collections.singletonList("c4")),
            new GraphEdgeRef("edge-5", "supports", "a2", "e5", Collections.singletonList("c5")));
        Map<String, TextChunk> chunks = new HashMap<>();
        for (int index = 1; index <= 5; index++) {
            String chunkId = "c" + index;
            chunks.put(chunkId, new TextChunk(chunkId, "doc-" + index, 0, 0, 1, 1, "fact"));
        }
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(2).candidateBudget(10).edgeScanBudget(5).neighborCap(5)
            .maxDiffusionGrowth(1.5).build();
        GraphRetriever retriever = new GraphRetriever(new EntityAnchorResolver(entities));

        List<GraphRetriever.GraphHit> hits = retriever.retrieve("Confucius Plato", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(5, hits.size(), "anchors=" + retriever.getLastStats().getAnchorsConsidered()
            + ", edges=" + retriever.getLastStats().getEdgesExamined());
        assertEquals(RecallStopReason.DIFFUSION_GROWTH_LIMIT, retriever.getLastStats().getStopReason());
        assertEquals(5, retriever.getLastStats().getEdgesExamined());
    }

    @Test
    void ordersHitsByAssociationScoreNotByTraversalOrder() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Weak", "topic"),
            new EntityRef("e3", "Strong", "topic"));
        // The weak edge is traversed first, so expansion order would place cWeak ahead of cStrong.
        // Ranking must follow the association score instead, because fusion turns the position into
        // a reciprocal-rank term.
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-weak", "weak", "e1", "e2", Collections.singletonList("cWeak")),
            new GraphEdgeRef("edge-strong", "strong", "e1", "e3",
                Collections.singletonList("cStrong")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("cWeak", new TextChunk("cWeak", "doc-1", 0, 0, 1, 1, "weak"));
        chunks.put("cStrong", new TextChunk("cStrong", "doc-2", 0, 0, 1, 1, "strong"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(1).candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .degreePenalty(0.0).relationPriority("weak", 1.0).relationPriority("strong", 4.0)
            .build();

        List<GraphRetriever.GraphHit> hits = new GraphRetriever().retrieve("Confucius", entities,
            new InMemoryGraphNeighborProvider(edges), chunks, config, 0L, System::nanoTime);

        assertEquals(2, hits.size());
        assertEquals("cStrong", hits.get(0).getSourceChunkIds().get(0),
            "the highest association score must rank first regardless of traversal order");
        assertEquals("cWeak", hits.get(1).getSourceChunkIds().get(0));
        assertEquals(4.0, hits.get(0).getScore(), 1e-9);
    }

    @Test
    void segmentedExpansionReturnsTheSameOrderAsTheSingleCall() {
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Ethics", "topic"),
            new EntityRef("e3", "Wisdom", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("edge-1", "teaches", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("edge-2", "supports", "e2", "e3", Collections.singletonList("c2")));
        Map<String, TextChunk> chunks = new HashMap<>();
        chunks.put("c1", new TextChunk("c1", "doc-1", 0, 0, 1, 1, "one"));
        chunks.put("c2", new TextChunk("c2", "doc-2", 0, 0, 1, 1, "two"));
        GraphRetrievalConfig config = GraphRetrievalConfig.builder()
            .maxHop(2).candidateBudget(10).edgeScanBudget(10).neighborCap(10)
            .hopDecay(0.5).degreePenalty(0.0).relationPriority("teaches", 2.0)
            .relationPriority("supports", 3.0).build();
        GraphRetriever retriever = new GraphRetriever();
        InMemoryGraphNeighborProvider provider = new InMemoryGraphNeighborProvider(edges);

        List<GraphRetriever.GraphHit> single = retriever.retrieve("Confucius", entities, provider,
            chunks, config, 0L, System::nanoTime);

        GraphRetriever segmentedRetriever = new GraphRetriever();
        GraphRetriever.ExpansionSession session = segmentedRetriever.beginRetrieval("Confucius", entities,
            provider, chunks, config, 0L, System::nanoTime, null, new ArrayList<>());
        while (!session.isTerminal() && !session.isExhausted()) {
            segmentedRetriever.expandOneHop(session, config, 0L, System::nanoTime);
        }
        List<GraphRetriever.GraphHit> segmented = session.finish();

        assertEquals(single.size(), segmented.size());
        for (int index = 0; index < single.size(); index++) {
            assertEquals(single.get(index).getSourceChunkIds(), segmented.get(index).getSourceChunkIds(),
                "hop-split expansion must produce the same ranked order as a single call");
            assertEquals(single.get(index).getScore(), segmented.get(index).getScore(), 1e-9);
        }
    }

    private static GraphEdgeRef edge(int index) {
        return new GraphEdgeRef(String.format("edge-%06d", index), "supports", "e1", "e2",
            Collections.singletonList("c1"));
    }
}
