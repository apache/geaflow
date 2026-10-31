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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;

/** Bounded deterministic one-hop graph expansion over fixture graph records. */
public final class GraphRetriever {
    private final EntityAnchorResolver resolver;
    private GraphStats lastStats = new GraphStats();

    public GraphRetriever() {
        this(new EntityAnchorResolver());
    }

    public GraphRetriever(EntityAnchorResolver resolver) {
        this.resolver = resolver;
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
                    lastStats.stopReason = RecallStopReason.EDGE_SCAN_LIMIT;
                    return result;
                }
                GraphEdgeRef edge = iterator.next();
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
                        lastStats.stopReason = RecallStopReason.CANDIDATE_LIMIT;
                        return result;
                    }
                    continue;
                }
                producedChunks.addAll(newCandidates);
                GraphPathRef path = new GraphPathRef(
                    java.util.Arrays.asList(anchor.getEntity().getEntityId(), target),
                    Collections.singletonList(edge.getEdgeId()), 1, false);
                result.add(new GraphHit(anchor, edge, path, boundedSources,
                    1.0 / (1.0 + result.size())));
                lastStats.verticesReached++;
                lastStats.candidatesProduced = producedChunks.size();
                if (producedChunks.size() >= candidateCap || provenanceLimitReached) {
                    lastStats.stopReason = RecallStopReason.CANDIDATE_LIMIT;
                    return result;
                }
            }
        }
        if (!lastStats.validationErrors.isEmpty() && lastStats.stopReason == RecallStopReason.COMPLETED) {
            lastStats.stopReason = RecallStopReason.TRACE_VALIDATION_ERROR;
        }
        return result;
    }

    public GraphStats getLastStats() {
        return lastStats;
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
        private final GraphEdgeRef edge;
        private final GraphPathRef path;
        private final List<String> sourceChunkIds;
        private final double score;

        private GraphHit(EntityAnchorResolver.Anchor anchor, GraphEdgeRef edge, GraphPathRef path,
                         List<String> sourceChunkIds, double score) {
            this.anchor = anchor;
            this.edge = edge;
            this.path = path;
            this.sourceChunkIds = new ArrayList<>(sourceChunkIds);
            this.score = score;
        }

        public EntityAnchorResolver.Anchor getAnchor() {
            return anchor;
        }

        public GraphEdgeRef getEdge() {
            return edge;
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
        private int candidatesProduced;
        private RecallStopReason stopReason = RecallStopReason.COMPLETED;
        private final List<String> validationErrors = new ArrayList<>();

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

        public int getCandidatesProduced() {
            return candidatesProduced;
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
