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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Neighbor provider backed by compact edge-index lists instead of copied adjacency records. */
public final class IndexedGraphNeighborProvider implements GraphNeighborProvider {

    private final GraphVersion graphVersion;
    private final List<EntityRef> entities;
    private final List<GraphEdgeRef> edges;
    private final Map<String, int[]> edgeIndexesByEntity;
    private final Map<String, GraphVertexRef> vertices;
    private final GraphStatistics statistics;
    private volatile String fingerprint;

    public IndexedGraphNeighborProvider(GraphVersion graphVersion, List<EntityRef> entities,
                                        List<GraphVertexRef> graphVertices,
                                        List<GraphEdgeRef> graphEdges) {
        this(graphVersion, entities, graphVertices, graphEdges,
            boxedIndexes(indexEdges(graphEdges)), GraphStatistics.from(graphEdges));
    }

    public IndexedGraphNeighborProvider(GraphVersion graphVersion, List<EntityRef> entities,
                                        List<GraphVertexRef> graphVertices,
                                        List<GraphEdgeRef> graphEdges,
                                        Map<String, List<Integer>> edgeIndexes,
                                        GraphStatistics statistics) {
        this(graphVersion, entities, graphVertices, graphEdges, edgeIndexes, statistics, false);
    }

    public static IndexedGraphNeighborProvider fromImmutableEdgeList(
        GraphVersion graphVersion, List<EntityRef> entities, List<GraphVertexRef> graphVertices,
        List<GraphEdgeRef> graphEdges, Map<String, List<Integer>> edgeIndexes,
        GraphStatistics statistics) {
        return new IndexedGraphNeighborProvider(graphVersion, entities, graphVertices, graphEdges,
            edgeIndexes, statistics, true);
    }

    private IndexedGraphNeighborProvider(GraphVersion graphVersion, List<EntityRef> entities,
                                         List<GraphVertexRef> graphVertices,
                                         List<GraphEdgeRef> graphEdges,
                                         Map<String, List<Integer>> edgeIndexes,
                                         GraphStatistics statistics, boolean immutableEdgeList) {
        this.graphVersion = graphVersion;
        this.entities = Collections.unmodifiableList(new ArrayList<>(entities));
        this.edges = immutableEdgeList ? Collections.unmodifiableList(graphEdges)
            : Collections.unmodifiableList(new ArrayList<>(graphEdges));
        this.edgeIndexesByEntity = toArrays(edgeIndexes);
        this.vertices = indexVertices(graphVertices);
        this.statistics = statistics;
    }

    private static Map<String, int[]> indexEdges(List<GraphEdgeRef> edges) {
        Map<String, List<Integer>> indexes = new HashMap<>();
        for (int index = 0; index < edges.size(); index++) {
            GraphEdgeRef edge = edges.get(index);
            indexes.computeIfAbsent(edge.getSourceEntityId(), ignored -> new ArrayList<>()).add(index);
            if (!edge.getSourceEntityId().equals(edge.getTargetEntityId())) {
                indexes.computeIfAbsent(edge.getTargetEntityId(), ignored -> new ArrayList<>()).add(index);
            }
        }
        return toArrays(indexes);
    }

    private static Map<String, List<Integer>> boxedIndexes(Map<String, int[]> indexes) {
        Map<String, List<Integer>> result = new HashMap<>();
        for (Map.Entry<String, int[]> entry : indexes.entrySet()) {
            List<Integer> values = new ArrayList<>(entry.getValue().length);
            for (int index : entry.getValue()) {
                values.add(index);
            }
            result.put(entry.getKey(), values);
        }
        return result;
    }

    private static Map<String, int[]> toArrays(Map<String, List<Integer>> indexes) {
        Map<String, int[]> result = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : indexes.entrySet()) {
            int[] values = new int[entry.getValue().size()];
            for (int index = 0; index < values.length; index++) {
                values[index] = entry.getValue().get(index);
            }
            result.put(entry.getKey(), values);
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, GraphVertexRef> indexVertices(List<GraphVertexRef> graphVertices) {
        Map<String, GraphVertexRef> result = new HashMap<>();
        for (GraphVertexRef vertex : graphVertices) {
            result.put(vertex.getEntityId(), vertex);
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<GraphEdgeRef> neighbors(String entityId) {
        int[] indexes = edgeIndexesByEntity.get(entityId);
        if (indexes == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new AbstractList<GraphEdgeRef>() {
            @Override
            public GraphEdgeRef get(int index) {
                return edges.get(indexes[index]);
            }

            @Override
            public int size() {
                return indexes.length;
            }
        });
    }

    @Override
    public List<GraphEdgeRef> prioritizedNeighbors(String entityId, Comparator<GraphEdgeRef> priority,
                                                   int limit) {
        List<GraphEdgeRef> values = neighbors(entityId);
        return NeighborBatch.select(values, priority, limit, values.size()).getEdges();
    }

    @Override
    public NeighborBatch prioritizedNeighbors(String entityId, Comparator<GraphEdgeRef> priority,
                                               int candidateLimit, int scanLimit) {
        return NeighborBatch.select(neighbors(entityId), priority, candidateLimit, scanLimit);
    }

    @Override
    public GraphVertexRef vertex(EntityRef entity) {
        GraphVertexRef vertex = vertices.get(entity.getEntityId());
        return vertex == null ? GraphNeighborProvider.super.vertex(entity) : vertex;
    }

    @Override
    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    @Override
    public GraphStatistics getStatistics() {
        return statistics;
    }

    @Override
    public String getStatisticsVersion() {
        return statistics.getStatisticsVersion();
    }

    @Override
    public String getFingerprint() {
        String result = fingerprint;
        if (result == null) {
            GraphSnapshot snapshot = new GraphSnapshot(graphVersion, Collections.emptyList(),
                entities, new ArrayList<>(vertices.values()), edges);
            result = GraphSnapshot.sha256(snapshot.getFingerprint() + "\n\n"
                + statistics.getStatisticsVersion());
            fingerprint = result;
        }
        return result;
    }
}
