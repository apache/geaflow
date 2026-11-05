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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.execution.BudgetLedger;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;

/** Bounded deterministic one-hop graph expansion over fixture graph records. */
public final class GraphRetriever {

    /**
     * Source relevance when an edge and its anchor share no source chunk.
     *
     * <p>Now supplied by {@link GraphRetrievalConfig} so the value is recorded in build metadata
     * instead of being a constant that made a published association score impossible to reproduce.
     * This constant remains only as the documented default.
     */
    private static final double UNRELATED_SOURCE_RELEVANCE = 0.5D;
    /**
     * Documented default base source-relevance score; see {@link GraphRetrievalConfig}.
     */
    private static final double ANCHOR_HIT_BASE_SCORE = 1.0D;
    private final EntityAnchorResolver resolver;
    private final TokenEstimator tokenEstimator;
    private GraphStats lastStats = new GraphStats();

    public GraphRetriever() {
        this(new EntityAnchorResolver(), new DeterministicTokenEstimator());
    }

    public GraphRetriever(EntityAnchorResolver resolver) {
        this(resolver, new DeterministicTokenEstimator());
    }

    public GraphRetriever(EntityAnchorResolver resolver, TokenEstimator tokenEstimator) {
        this.resolver = resolver;
        this.tokenEstimator = java.util.Objects.requireNonNull(tokenEstimator, "tokenEstimator");
    }

    public List<GraphHit> retrieve(String query, List<EntityRef> entities, List<GraphEdgeRef> edges,
                                   Map<String, TextChunk> chunks, int topK, int edgeCap,
                                   long deadlineNanos) {
        return retrieve(query, entities, new InMemoryGraphNeighborProvider(edges), chunks,
            topK, edgeCap, deadlineNanos, System::nanoTime);
    }

    public List<GraphHit> retrieve(String query, List<EntityRef> entities, List<GraphEdgeRef> edges,
                                  Map<String, TextChunk> chunks, int candidateCap, int edgeCap,
                                  long deadlineNanos, LongSupplier clock) {
        return retrieve(query, entities, new InMemoryGraphNeighborProvider(edges), chunks,
            candidateCap, edgeCap, deadlineNanos, clock);
    }

    public List<GraphHit> retrieve(String query, List<EntityRef> entities, GraphNeighborProvider provider,
                                   Map<String, TextChunk> chunks, int candidateCap, int edgeCap,
                                   long deadlineNanos, LongSupplier clock) {
        lastStats = new GraphStats();
        if (candidateCap <= 0 || edgeCap <= 0 || entities == null || provider == null || chunks == null) {
            return Collections.emptyList();
        }
        if (expired(deadlineNanos, clock)) {
            return Collections.emptyList();
        }
        List<EntityAnchorResolver.Anchor> anchors = resolver.resolve(query, entities, deadlineNanos, clock);
        if (expired(deadlineNanos, clock)) {
            return Collections.emptyList();
        }
        lastStats.anchorsConsidered = anchors.size();
        if (anchors.isEmpty()) {
            lastStats.stopReason = RecallStopReason.NO_RELIABLE_ANCHOR;
            return Collections.emptyList();
        }
        Map<String, EntityRef> entityMap = new HashMap<>();
        for (EntityRef entity : entities) {
            if (entity != null) {
                entityMap.put(entity.getEntityId(), entity);
            }
        }
        List<GraphHit> result = new ArrayList<>();
        Set<String> seenEdges = new HashSet<>();
        Set<String> producedChunks = new HashSet<>();
        int provenanceIdsExamined = 0;
        for (EntityAnchorResolver.Anchor anchor : anchors) {
            if (expired(deadlineNanos, clock)) {
                return result;
            }
            List<GraphEdgeRef> neighbors = provider.neighbors(anchor.getEntity().getEntityId());
            if (neighbors == null) {
                continue;
            }
            java.util.Iterator<GraphEdgeRef> iterator = neighbors.iterator();
            while (iterator.hasNext()) {
                if (expired(deadlineNanos, clock)) {
                    return result;
                }
                if (lastStats.edgesExamined >= edgeCap) {
                    stopForBudget(RecallStopReason.EDGE_SCAN_LIMIT);
                    return result;
                }
                final GraphEdgeRef edge = iterator.next();
                lastStats.neighborsSampled++;
                lastStats.edgesExamined++;
                if (edge == null) {
                    continue;
                }
                if (!anchor.getEntity().getEntityId().equals(edge.getSourceEntityId())
                    && !anchor.getEntity().getEntityId().equals(edge.getTargetEntityId())) {
                    continue;
                }
                if (!seenEdges.add(edge.getEdgeId())) {
                    continue;
                }
                String target = anchor.getEntity().getEntityId().equals(edge.getSourceEntityId())
                    ? edge.getTargetEntityId() : edge.getSourceEntityId();
                if (!entityMap.containsKey(target)) {
                    continue;
                }
                List<String> sourceIds = edge.getSourceChunkIds();
                if (sourceIds.isEmpty()) {
                    sourceIds = anchor.getEntity().getSourceChunkIds();
                }
                if (sourceIds.isEmpty()) {
                    lastStats.validationErrors.add("edge " + edge.getEdgeId() + " references an unknown chunk");
                    continue;
                }
                List<String> boundedSources = new ArrayList<>();
                Set<String> edgeSources = new HashSet<>();
                Set<String> newCandidates = new HashSet<>();
                boolean invalidSource = false;
                boolean provenanceLimitReached = false;
                for (String id : sourceIds) {
                    if (expired(deadlineNanos, clock)) {
                        return result;
                    }
                    if (provenanceIdsExamined >= candidateCap) {
                        provenanceLimitReached = true;
                        break;
                    }
                    provenanceIdsExamined++;
                    if (!chunks.containsKey(id)) {
                        invalidSource = true;
                        break;
                    }
                    if (edgeSources.add(id)) {
                        boundedSources.add(id);
                    }
                    if (!producedChunks.contains(id)) {
                        newCandidates.add(id);
                    }
                    if (producedChunks.size() + newCandidates.size() >= candidateCap) {
                        break;
                    }
                }
                if (invalidSource) {
                    lastStats.validationErrors.add("edge " + edge.getEdgeId() + " references an unknown chunk");
                    continue;
                }
                if (boundedSources.isEmpty()) {
                    if (provenanceLimitReached) {
                        stopForBudget(RecallStopReason.CANDIDATE_LIMIT);
                        return result;
                    }
                    continue;
                }
                producedChunks.addAll(newCandidates);
                GraphVertexRef sourceVertex = provider.vertex(anchor.getEntity());
                GraphVertexRef targetVertex = provider.vertex(entityMap.get(target));
                List<GraphVertexRef> pathVertices = java.util.Arrays.asList(sourceVertex, targetVertex);
                GraphPathRef path = new GraphPathRef(pathVertices,
                    Collections.singletonList(edge), false);
                result.add(new GraphHit(anchor, path, boundedSources,
                    1.0 / (1.0 + result.size())));
                lastStats.verticesReached++;
                lastStats.candidatesProduced = producedChunks.size();
                if (producedChunks.size() >= candidateCap || provenanceLimitReached) {
                    stopForBudget(RecallStopReason.CANDIDATE_LIMIT);
                    return result;
                }
            }
        }
        if (!lastStats.validationErrors.isEmpty() && lastStats.stopReason == RecallStopReason.COMPLETED) {
            lastStats.stopReason = RecallStopReason.TRACE_VALIDATION_ERROR;
        }
        return result;
    }

    /** Executes a deterministic, bounded frontier expansion. */
    public List<GraphHit> retrieve(String query, List<EntityRef> entities, GraphNeighborProvider provider,
                                   Map<String, TextChunk> chunks, GraphRetrievalConfig config,
                                   long deadlineNanos, LongSupplier clock) {
        return retrieve(query, entities, provider, chunks, config, deadlineNanos, clock, null);
    }

    /** Executes graph expansion while charging unique source chunks to a request ledger. */
    public List<GraphHit> retrieve(String query, List<EntityRef> entities, GraphNeighborProvider provider,
                                   Map<String, TextChunk> chunks, GraphRetrievalConfig config,
                                   long deadlineNanos, LongSupplier clock, BudgetLedger ledger) {
        return retrieve(query, entities, provider, chunks, config, deadlineNanos, clock, ledger,
            Collections.emptyList());
    }

    /** Uses bounded text-channel candidates to resolve anchors when the question has no direct mention. */
    public List<GraphHit> retrieve(String query, List<EntityRef> entities, GraphNeighborProvider provider,
                                   Map<String, TextChunk> chunks, GraphRetrievalConfig config,
                                   long deadlineNanos, LongSupplier clock, BudgetLedger ledger,
                                   List<String> candidateChunkIds) {
        lastStats = new GraphStats();
        if (config == null || entities == null || provider == null || chunks == null) {
            return Collections.emptyList();
        }
        if (expired(deadlineNanos, clock)) {
            return Collections.emptyList();
        }
        List<EntityAnchorResolver.Anchor> anchors = resolver.resolveFromCandidates(query, entities,
            candidateChunkIds, Math.min(config.getNeighborCap(), config.getCandidateBudget()),
            deadlineNanos, clock);
        if (expired(deadlineNanos, clock)) {
            return Collections.emptyList();
        }
        lastStats.anchorsConsidered = anchors.size();
        if (anchors.isEmpty()) {
            lastStats.stopReason = RecallStopReason.NO_RELIABLE_ANCHOR;
            return Collections.emptyList();
        }
        Map<String, EntityRef> entityMap = new HashMap<>();
        for (EntityRef entity : entities) {
            if (entity != null) {
                entityMap.put(entity.getEntityId(), entity);
            }
        }
        PriorityQueue<ExpansionState> frontier = new PriorityQueue<>(ExpansionState.ORDER);
        Set<String> visitedVertices = new LinkedHashSet<>();
        for (EntityAnchorResolver.Anchor anchor : anchors) {
            String entityId = anchor.getEntity().getEntityId();
            visitedVertices.add(entityId);
            frontier.add(ExpansionState.seed(anchor, provider.vertex(anchor.getEntity()),
                anchor.getConfidence() * config.getAnchorScoreWeight()));
        }
        lastStats.verticesReached = visitedVertices.size();
        lastStats.uniqueVertices = visitedVertices.size();
        Set<String> scannedEdges = new HashSet<>();
        Set<String> producedChunks = new LinkedHashSet<>();
        Set<String> emittedPaths = new HashSet<>();
        List<GraphHit> result = new ArrayList<>();
        Map<Integer, Integer> frontierSizes = new HashMap<>();
        Map<Integer, Integer> edgesExaminedByHop = new HashMap<>();
        Map<Integer, Integer> newVerticesByHop = new HashMap<>();
        Map<Integer, Integer> newEvidenceByHop = new HashMap<>();
        frontierSizes.put(0, anchors.size());
        boolean reachedMaxHop = false;
        while (!frontier.isEmpty()) {
            if (expired(deadlineNanos, clock)) {
                return result;
            }
            ExpansionState state = frontier.poll();
            if (state.hop >= config.getMaxHop()) {
                reachedMaxHop = true;
                continue;
            }
            if (state.hop > 0 && lastStats.growthRate > config.getMaxDiffusionGrowth()) {
                stopWithValidation(RecallStopReason.DIFFUSION_GROWTH_LIMIT);
                continue;
            }
            int remainingEdgeBudget = config.getEdgeScanBudget() - lastStats.edgesExamined;
            GraphNeighborProvider.NeighborBatch batch = provider.prioritizedNeighbors(state.entityId,
                edgeComparator(query, state, config), config.getNeighborCap(), remainingEdgeBudget);
            if (expired(deadlineNanos, clock)) {
                return result;
            }
            int examined = batch.getExamined();
            lastStats.edgesExamined += examined;
            increment(edgesExaminedByHop, state.hop + 1, examined);
            int scoredThisHop = 0;
            int survivedThisHop = 0;
            boolean associationTerminated = false;
            for (GraphEdgeRef edge : batch.getEdges()) {
                if (expired(deadlineNanos, clock)) {
                    return result;
                }
                if (!sampled(query, state, edge, config)) {
                    continue;
                }
                lastStats.neighborsSampled++;
                if (!scannedEdges.add(edge.getEdgeId())) {
                    continue;
                }
                String targetId = otherEndpoint(state.entityId, edge);
                if (targetId == null || !entityMap.containsKey(targetId)) {
                    lastStats.validationErrors.add("edge " + edge.getEdgeId()
                        + " references an unknown entity");
                    continue;
                }
                if (state.pathEntityIds.contains(targetId)) {
                    continue;
                }
                double score = associationScore(state, edge, targetId, provider, config);
                scoredThisHop++;
                if (score < config.getMinAssociationScore()) {
                    lastStats.filteredByAssociation++;
                    associationTerminated = exceedsAssociationFloor(
                        survivedThisHop, scoredThisHop, config);
                    if (associationTerminated) {
                        break;
                    }
                    continue;
                }
                survivedThisHop++;
                List<String> sourceIds = sourceIds(state, edge, entityMap);
                if (sourceIds.isEmpty()) {
                    lastStats.validationErrors.add("edge " + edge.getEdgeId()
                        + " references an unknown chunk");
                    continue;
                }
                int remainingCandidates = config.getCandidateBudget() - producedChunks.size();
                if (remainingCandidates <= 0) {
                    stopForBudget(RecallStopReason.CANDIDATE_LIMIT);
                    return result;
                }
                int newChunks = 0;
                long addedTokens = 0L;
                boolean invalidSource = false;
                Set<String> edgeSources = new LinkedHashSet<>();
                List<String> boundedSourceIds = new ArrayList<>();
                for (String sourceId : sourceIds) {
                    if (!edgeSources.add(sourceId)) {
                        continue;
                    }
                    TextChunk chunk = chunks.get(sourceId);
                    if (chunk == null) {
                        invalidSource = true;
                        break;
                    }
                    if (!producedChunks.contains(sourceId)) {
                        int estimate = tokenEstimator.estimate(chunk);
                        if (estimate < 1) {
                            lastStats.validationErrors.add("token estimator returned a non-positive estimate");
                            stopForBudget(RecallStopReason.TOKEN_BUDGET);
                            return result;
                        }
                        if (ledger != null) {
                            if (!ledger.tryRetain(sourceId, chunk, tokenEstimator)) {
                                stopForBudget(ledger.getStopReason());
                                return result;
                            }
                        } else {
                            long projectedTokens = (long) lastStats.tokensEstimated + addedTokens + estimate;
                            if (projectedTokens > config.getTokenBudget()) {
                                stopForBudget(RecallStopReason.TOKEN_BUDGET);
                                return result;
                            }
                        }
                        addedTokens += estimate;
                        newChunks++;
                    }
                    boundedSourceIds.add(sourceId);
                    if (newChunks >= remainingCandidates) {
                        break;
                    }
                }
                if (invalidSource) {
                    lastStats.validationErrors.add("edge " + edge.getEdgeId()
                        + " references an unknown chunk");
                    continue;
                }
                sourceIds = boundedSourceIds;
                GraphVertexRef targetVertex = provider.vertex(entityMap.get(targetId));
                int hop = state.hop + 1;
                Set<String> pathSources = new LinkedHashSet<>(state.sourceChunkIds);
                pathSources.addAll(sourceIds);
                List<String> orderedSources = new ArrayList<>(pathSources);
                Collections.sort(orderedSources);
                ExpansionState nextState = state.extend(targetId, targetVertex, edge, score,
                    orderedSources, config.getSampleRate() < 1.0);
                GraphPathRef path = nextState.path;
                if (!emittedPaths.add(pathKey(path))) {
                    continue;
                }
                producedChunks.addAll(sourceIds);
                lastStats.tokensEstimated += (int) addedTokens;
                lastStats.candidatesProduced = producedChunks.size();
                lastStats.evidenceYield += newChunks;
                boolean newVertex = visitedVertices.add(targetId);
                if (newVertex) {
                    lastStats.verticesReached = visitedVertices.size();
                    lastStats.uniqueVertices = visitedVertices.size();
                    increment(newVerticesByHop, hop);
                }
                increment(newEvidenceByHop, hop, newChunks);
                lastStats.growthRate = (double) newVerticesByHop.getOrDefault(hop, 0)
                    / Math.max(1, frontierSizes.getOrDefault(hop - 1, 1));
                lastStats.marginalYield = (double) newEvidenceByHop.getOrDefault(hop, 0)
                    / Math.max(1, edgesExaminedByHop.getOrDefault(hop, 0));
                result.add(new GraphHit(state.anchor, path, orderedSources, score));
                if (producedChunks.size() >= config.getCandidateBudget()) {
                    stopForBudget(RecallStopReason.CANDIDATE_LIMIT);
                    return result;
                }
                if (lastStats.growthRate > config.getMaxDiffusionGrowth()) {
                    stopWithValidation(RecallStopReason.DIFFUSION_GROWTH_LIMIT);
                    continue;
                }
                if (hop < config.getMaxHop()) {
                    increment(frontierSizes, hop);
                    frontier.add(nextState);
                } else {
                    reachedMaxHop = true;
                }
                if (lastStats.marginalYield < config.getMinMarginalYield()) {
                    stopWithValidation(RecallStopReason.MARGINAL_YIELD_LIMIT);
                    return result;
                }
            }
            if (batch.hasMore()) {
                stopForBudget(RecallStopReason.EDGE_SCAN_LIMIT);
                return result;
            }
            if (associationTerminated) {
                lastStats.terminatedHop = state.hop + 1;
                stopWithValidation(RecallStopReason.LOW_ASSOCIATION_SCORE);
                return result;
            }
        }
        if (lastStats.stopReason == RecallStopReason.COMPLETED && !lastStats.validationErrors.isEmpty()) {
            lastStats.stopReason = RecallStopReason.TRACE_VALIDATION_ERROR;
        } else if (lastStats.stopReason == RecallStopReason.COMPLETED && reachedMaxHop) {
            lastStats.stopReason = RecallStopReason.MAX_HOP_REACHED;
        }
        return result;
    }

    public List<GraphHit> retrieve(String query, List<EntityRef> entities, GraphNeighborProvider provider,
                                   Map<String, TextChunk> chunks, GraphRetrievalConfig config,
                                   long deadlineNanos) {
        return retrieve(query, entities, provider, chunks, config, deadlineNanos, System::nanoTime);
    }

    /**
     * Returns true when too few scored edges survived the association floor for this hop to keep
     * consuming the edge-scan budget. A survival rate of 1.0 disables the early stop.
     */
    private static boolean exceedsAssociationFloor(int survived, int scored,
                                                   GraphRetrievalConfig config) {
        double floor = config.getMinAssociationSurvivalRate();
        if (floor >= 1.0D || scored == 0) {
            return false;
        }
        return (double) survived / (double) scored < floor;
    }

    private static boolean sampled(String query, ExpansionState state, GraphEdgeRef edge,
                                   GraphRetrievalConfig config) {
        return sampledEdge(query, state.anchor.getEntity().getEntityId(), state.entityId, edge, config);
    }

    private static boolean sampledEdge(String query, String anchorEntityId, String currentEntityId,
                                       GraphEdgeRef edge, GraphRetrievalConfig config) {
        if (config.getSampleRate() >= 1.0) {
            return true;
        }
        String key = query + "\u001f" + anchorEntityId + "\u001f"
            + currentEntityId + "\u001f" + otherEndpoint(currentEntityId, edge) + "\u001f"
            + config.getSamplingSeed();
        long value = Long.parseUnsignedLong(GraphSnapshot.sha256(key).substring(0, 16), 16);
        double normalized = (value >>> 1) / (double) Long.MAX_VALUE;
        return normalized < config.getSampleRate();
    }

    private static Comparator<GraphEdgeRef> edgeComparator(String query, ExpansionState state,
                                                            GraphRetrievalConfig config) {
        return Comparator.comparingDouble((GraphEdgeRef edge) ->
                config.relationPriority(edge.getLabel())).reversed()
            .thenComparing(edge -> edgeOrderKey(query, state, edge, config))
            .thenComparing(GraphEdgeRef::getEdgeId);
    }

    private static String edgeOrderKey(String query, ExpansionState state, GraphEdgeRef edge,
                                       GraphRetrievalConfig config) {
        String target = otherEndpoint(state.entityId, edge);
        return GraphSnapshot.sha256(query + "\u001f" + state.anchor.getEntity().getEntityId()
            + "\u001f" + state.entityId + "\u001f" + target + "\u001f"
            + config.getSamplingSeed());
    }

    private static String otherEndpoint(String current, GraphEdgeRef edge) {
        if (current.equals(edge.getSourceEntityId())) {
            return edge.getTargetEntityId();
        }
        if (current.equals(edge.getTargetEntityId())) {
            return edge.getSourceEntityId();
        }
        return null;
    }

    private static double associationScore(ExpansionState state, GraphEdgeRef edge, String targetId,
                                           GraphNeighborProvider provider, GraphRetrievalConfig config) {
        double edgeStrength = EdgeStrength.of(config.relationPriority(edge.getLabel()),
            reinforcementCount(provider, edge), ageMillis(provider, edge),
            config.getTimeDecayWeight(), config.getTimeDecayHalfLifeMillis())
            * config.getEdgeStrengthWeight();
        double relevance = sourceRelevance(state.anchor.getEntity(), edge, config)
            * config.getSourceRelevanceWeight();
        GraphStatistics statistics = provider.getStatistics();
        int degree = statistics == null ? 0 : statistics.getDegree(targetId);
        // The first edge is hop one and therefore has no decay: the state at hop n enters hop n + 1.
        return AssociationScore.score(state.score, state.hop + 1, edgeStrength, degree,
            config.getDegreePenalty(), config.getHopDecay(), relevance);
    }

    private static int reinforcementCount(GraphNeighborProvider provider, GraphEdgeRef edge) {
        return provider instanceof EdgeReinforcementSource
            ? ((EdgeReinforcementSource) provider).getReinforcementCount(edge) : 0;
    }

    private static long ageMillis(GraphNeighborProvider provider, GraphEdgeRef edge) {
        return provider instanceof EdgeReinforcementSource
            ? ((EdgeReinforcementSource) provider).getAgeMillis(edge) : 0L;
    }

    private static double sourceRelevance(EntityRef anchor, GraphEdgeRef edge,
                                          GraphRetrievalConfig config) {
        if (anchor.getSourceChunkIds().isEmpty() || edge.getSourceChunkIds().isEmpty()) {
            return 1.0;
        }
        Set<String> anchorSources = new HashSet<>(anchor.getSourceChunkIds());
        int overlap = 0;
        for (String source : edge.getSourceChunkIds()) {
            if (anchorSources.contains(source)) {
                overlap++;
            }
        }
        return overlap == 0 ? config.getUnrelatedSourceRelevance()
            : config.getAnchorHitBaseScore() + (double) overlap / anchorSources.size();
    }

    private static List<String> sourceIds(ExpansionState state, GraphEdgeRef edge,
                                          Map<String, EntityRef> entityMap) {
        if (!edge.getSourceChunkIds().isEmpty()) {
            return edge.getSourceChunkIds();
        }
        EntityRef entity = entityMap.get(state.entityId);
        return entity == null ? Collections.emptyList() : entity.getSourceChunkIds();
    }

    private static String pathKey(GraphPathRef path) {
        return path.getVertexIds().toString() + "\u001f" + path.getEdgeIds();
    }

    private static void increment(Map<Integer, Integer> counters, int key) {
        increment(counters, key, 1);
    }

    private static void increment(Map<Integer, Integer> counters, int key, int amount) {
        counters.put(key, counters.getOrDefault(key, 0) + amount);
    }

    private static final class ExpansionState {
        private static final Comparator<ExpansionState> ORDER = Comparator
            .comparingInt((ExpansionState value) -> value.hop)
            .thenComparing(Comparator.comparingDouble((ExpansionState value) -> value.score).reversed())
            .thenComparing(value -> value.entityId)
            .thenComparing(value -> value.path.getEdgeIds().toString());
        private final EntityAnchorResolver.Anchor anchor;
        private final String entityId;
        private final int hop;
        private final double score;
        private final GraphPathRef path;
        private final Set<String> pathEntityIds;
        private final List<String> sourceChunkIds;

        private ExpansionState(EntityAnchorResolver.Anchor anchor, String entityId,
                               int hop, double score, GraphPathRef path, Set<String> pathEntityIds,
                               List<String> sourceChunkIds) {
            this.anchor = anchor;
            this.entityId = entityId;
            this.hop = hop;
            this.score = score;
            this.path = path;
            this.pathEntityIds = pathEntityIds;
            this.sourceChunkIds = sourceChunkIds;
        }

        private static ExpansionState seed(EntityAnchorResolver.Anchor anchor, GraphVertexRef vertex,
                                           double score) {
            Set<String> entityIds = new LinkedHashSet<>();
            entityIds.add(anchor.getEntity().getEntityId());
            return new ExpansionState(anchor, anchor.getEntity().getEntityId(), 0, score,
                new GraphPathRef(Collections.singletonList(vertex), Collections.emptyList(), false),
                entityIds, new ArrayList<>());
        }

        private ExpansionState extend(String targetId, GraphVertexRef vertex, GraphEdgeRef edge,
                                      double nextScore, List<String> sources, boolean sampled) {
            List<GraphVertexRef> vertices = new ArrayList<>(path.getVertices());
            vertices.add(vertex);
            List<GraphEdgeRef> edges = new ArrayList<>(path.getEdges());
            edges.add(edge);
            Set<String> entityIds = new LinkedHashSet<>(pathEntityIds);
            entityIds.add(targetId);
            List<String> allSources = new ArrayList<>(sourceChunkIds);
            allSources.addAll(sources);
            return new ExpansionState(anchor, targetId, hop + 1, nextScore,
                new GraphPathRef(vertices, edges, sampled || path.isSampled()), entityIds, allSources);
        }
    }

    public GraphStats getLastStats() {
        return lastStats;
    }

    private void stopForBudget(RecallStopReason budgetReason) {
        stopWithValidation(budgetReason);
    }

    private void stopWithValidation(RecallStopReason reason) {
        lastStats.stopReason = lastStats.validationErrors.isEmpty()
            ? reason : RecallStopReason.TRACE_VALIDATION_ERROR;
    }

    private boolean expired(long deadline, LongSupplier clock) {
        if (deadline != 0L && clock.getAsLong() - deadline >= 0L) {
            lastStats.stopReason = RecallStopReason.DEADLINE;
            return true;
        }
        return false;
    }

    /** Graph candidate with path and source provenance. */
    public static final class GraphHit {
        private final EntityAnchorResolver.Anchor anchor;
        private final GraphPathRef path;
        private final List<String> sourceChunkIds;
        private final double score;

        private GraphHit(EntityAnchorResolver.Anchor anchor, GraphPathRef path,
                         List<String> sourceChunkIds, double score) {
            this.anchor = anchor;
            this.path = path;
            this.sourceChunkIds = new ArrayList<>(sourceChunkIds);
            this.score = score;
        }

        public EntityAnchorResolver.Anchor getAnchor() {
            return anchor;
        }

        public GraphVertexRef getSeed() {
            return path.getVertices().get(0);
        }

        public GraphPathRef getPath() {
            return path;
        }

        public List<String> getSourceChunkIds() {
            return Collections.unmodifiableList(sourceChunkIds);
        }

        public double getScore() {
            return score;
        }
    }

    /** Execution counters for trace construction. */
    public static final class GraphStats {
        private int anchorsConsidered;
        private int edgesExamined;
        private int neighborsSampled;
        private int verticesReached;
        private int uniqueVertices;
        private int candidatesProduced;
        private int tokensEstimated;
        private int evidenceYield;
        private double growthRate;
        private double marginalYield;
        private int filteredByAssociation;
        private int terminatedHop = -1;
        private RecallStopReason stopReason = RecallStopReason.COMPLETED;
        private final List<String> validationErrors = new ArrayList<>();

        public int getFilteredByAssociation() {
            return filteredByAssociation;
        }

        /** Hop index that was abandoned because too few edges passed the association floor. */
        public int getTerminatedHop() {
            return terminatedHop;
        }

        public int getAnchorsConsidered() {
            return anchorsConsidered;
        }

        public int getEdgesExamined() {
            return edgesExamined;
        }

        public int getNeighborsSampled() {
            return neighborsSampled;
        }

        public int getVerticesReached() {
            return verticesReached;
        }

        public int getUniqueVertices() {
            return uniqueVertices;
        }

        public int getCandidatesProduced() {
            return candidatesProduced;
        }

        public int getTokensEstimated() {
            return tokensEstimated;
        }

        public int getEdgesScanned() {
            return edgesExamined;
        }

        public int getNeighborsSampledCount() {
            return neighborsSampled;
        }

        public int getEvidenceYield() {
            return evidenceYield;
        }

        public double getGrowthRate() {
            return growthRate;
        }

        public double getMarginalYield() {
            return marginalYield;
        }

        public RecallStopReason getStopReason() {
            return stopReason;
        }

        public List<String> getValidationErrors() {
            return Collections.unmodifiableList(validationErrors);
        }

        public boolean isDeadlineReached() {
            return stopReason == RecallStopReason.DEADLINE;
        }
    }
}
