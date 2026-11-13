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
import java.util.List;
import java.util.PriorityQueue;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Versioned graph boundary used by the bounded graph retriever. */
public interface GraphNeighborProvider {

    /** Returns non-null neighbors ordered by edge ID, source, target, and label. */
    List<GraphEdgeRef> neighbors(String entityId);

    /**
     * Returns up to {@code limit} prioritized neighbors from a bounded prefix scan.
     */
    default List<GraphEdgeRef> prioritizedNeighbors(String entityId,
                                                     Comparator<GraphEdgeRef> priority,
                                                     int limit) {
        return prioritizedNeighbors(entityId, priority, limit, limit).getEdges();
    }

    /** Selects a prioritized page while bounding the number of adjacency records examined. */
    default NeighborBatch prioritizedNeighbors(String entityId, Comparator<GraphEdgeRef> priority,
                                                int candidateLimit, int scanLimit) {
        List<GraphEdgeRef> values = neighbors(entityId);
        if (values == null || values.isEmpty()) {
            return new NeighborBatch(Collections.emptyList(), 0, false);
        }
        if (candidateLimit <= 0) {
            return new NeighborBatch(Collections.emptyList(), 0, false);
        }
        if (scanLimit <= 0) {
            return new NeighborBatch(Collections.emptyList(), 0, true);
        }
        NeighborBatch batch = NeighborBatch.select(values, priority, candidateLimit,
            Math.min(candidateLimit, scanLimit));
        return new NeighborBatch(batch.getEdges(), batch.getExamined(),
            candidateLimit >= scanLimit && batch.hasMore());
    }

    /** Bounded selection result; examined counts records inspected before priority selection. */
    final class NeighborBatch {
        private final List<GraphEdgeRef> edges;
        private final int examined;
        private final boolean hasMore;

        private NeighborBatch(List<GraphEdgeRef> edges, int examined, boolean hasMore) {
            this.edges = edges;
            this.examined = examined;
            this.hasMore = hasMore;
        }

        static NeighborBatch select(List<GraphEdgeRef> values, Comparator<GraphEdgeRef> priority,
                                    int candidateLimit, int scanLimit) {
            if (values == null || values.isEmpty() || candidateLimit <= 0) {
                return new NeighborBatch(Collections.emptyList(), 0, false);
            }
            if (scanLimit <= 0) {
                return new NeighborBatch(Collections.emptyList(), 0, true);
            }
            PriorityQueue<GraphEdgeRef> best = new PriorityQueue<>(candidateLimit, priority.reversed());
            int examined = 0;
            java.util.Iterator<GraphEdgeRef> iterator = values.iterator();
            while (iterator.hasNext() && examined < scanLimit) {
                GraphEdgeRef edge = iterator.next();
                examined++;
                if (edge == null) {
                    continue;
                }
                if (best.size() < candidateLimit) {
                    best.add(edge);
                } else if (priority.compare(edge, best.peek()) < 0) {
                    best.poll();
                    best.add(edge);
                }
            }
            List<GraphEdgeRef> result = new ArrayList<>(best);
            result.sort(priority);
            return new NeighborBatch(Collections.unmodifiableList(result), examined, iterator.hasNext());
        }

        public List<GraphEdgeRef> getEdges() {
            return edges;
        }

        public int getExamined() {
            return examined;
        }

        public boolean hasMore() {
            return hasMore;
        }
    }

    /** Returns the graph vertex identity for an entity; providers may preserve distinct IDs. */
    default GraphVertexRef vertex(EntityRef entity) {
        return new GraphVertexRef(entity.getType(), entity.getEntityId(), entity.getEntityId(),
            entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(), entity.getSourceHash());
    }

    /** Returns the pinned graph version when the provider is version-aware. */
    default GraphVersion getGraphVersion() {
        return null;
    }

    /** Returns the deterministic digest of applied deltas, or an empty value for a snapshot. */
    default String getDeltaDigest() {
        return "";
    }

    /** Returns the statistics build version, or an empty value for a legacy provider. */
    default String getStatisticsVersion() {
        return "";
    }

    /** Returns deterministic local graph statistics when available. */
    default GraphStatistics getStatistics() {
        return GraphStatistics.from(java.util.Collections.emptyList());
    }

    /** Returns a stable reader lease over this provider. */
    default GraphQueryLease openLease() {
        return new GraphQueryLease(this);
    }

    /** Returns a stable fingerprint for the logical graph view. */
    default String getFingerprint() {
        return "";
    }
}
