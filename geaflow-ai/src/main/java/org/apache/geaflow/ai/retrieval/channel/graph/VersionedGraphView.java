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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Immutable logical graph assembled from one snapshot and an ordered delta chain. */
public final class VersionedGraphView implements GraphNeighborProvider {

    private final GraphSnapshot snapshot;
    private final GraphVersion graphVersion;
    private final String deltaCanonical;
    private final String deltaDigest;
    private final GraphStatistics statistics;
    private final Map<String, List<GraphEdgeRef>> neighbors;
    private final Map<String, GraphVertexRef> vertices;
    private final String fingerprint;

    private VersionedGraphView(GraphSnapshot snapshot, List<GraphDelta> deltas) {
        GraphSnapshot current = snapshot;
        StringBuilder digest = new StringBuilder();
        for (GraphDelta delta : deltas) {
            current = delta.applyTo(current);
            digest.append(delta.getDigest()).append('\n');
        }
        this.snapshot = current;
        this.graphVersion = current.getGraphVersion();
        this.deltaCanonical = digest.toString();
        this.deltaDigest = deltaCanonical.isEmpty() ? "" : GraphSnapshot.sha256(deltaCanonical);
        this.statistics = current.getStatistics();
        this.neighbors = buildNeighbors(current.getEdges());
        this.vertices = new HashMap<>();
        for (GraphVertexRef vertex : current.getVertices()) {
            vertices.put(vertex.getEntityId(), vertex);
        }
        this.fingerprint = GraphSnapshot.sha256(current.getFingerprint() + "\n" + deltaDigest
            + "\n" + statistics.getStatisticsVersion());
    }

    private VersionedGraphView(VersionedGraphView parent, GraphDelta delta) {
        this.snapshot = delta.applyTo(parent.snapshot);
        this.graphVersion = snapshot.getGraphVersion();
        this.deltaCanonical = parent.deltaCanonical + delta.getDigest() + '\n';
        this.deltaDigest = GraphSnapshot.sha256(deltaCanonical);
        this.statistics = snapshot.getStatistics();
        this.neighbors = buildNeighbors(snapshot.getEdges());
        this.vertices = new HashMap<>();
        for (GraphVertexRef vertex : snapshot.getVertices()) {
            vertices.put(vertex.getEntityId(), vertex);
        }
        this.fingerprint = GraphSnapshot.sha256(snapshot.getFingerprint() + "\n" + deltaDigest
            + "\n" + statistics.getStatisticsVersion());
    }

    public static VersionedGraphView from(GraphSnapshot snapshot, GraphDelta... deltas) {
        if (snapshot == null) {
            throw new GraphViewException(GraphViewException.Code.NOT_FOUND, "graph snapshot is required");
        }
        List<GraphDelta> values = new ArrayList<>();
        if (deltas != null) {
            for (GraphDelta delta : deltas) {
                if (delta == null) {
                    throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, "delta must not be null");
                }
                values.add(delta);
            }
        }
        return new VersionedGraphView(snapshot, values);
    }

    public static VersionedGraphView from(GraphSnapshot snapshot, List<GraphDelta> deltas) {
        return from(snapshot, deltas == null ? new GraphDelta[0] : deltas.toArray(new GraphDelta[0]));
    }

    public static VersionedGraphView from(VersionedGraphView parent, GraphDelta delta) {
        if (parent == null || delta == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "parent view and delta are required");
        }
        return new VersionedGraphView(parent, delta);
    }

    private static Map<String, List<GraphEdgeRef>> buildNeighbors(List<GraphEdgeRef> edges) {
        Map<String, List<GraphEdgeRef>> result = new HashMap<>();
        for (GraphEdgeRef edge : edges) {
            result.computeIfAbsent(edge.getSourceEntityId(), ignored -> new ArrayList<>()).add(edge);
            if (!Objects.equals(edge.getSourceEntityId(), edge.getTargetEntityId())) {
                result.computeIfAbsent(edge.getTargetEntityId(), ignored -> new ArrayList<>()).add(edge);
            }
        }
        for (List<GraphEdgeRef> value : result.values()) {
            value.sort((left, right) -> left.getEdgeId().compareTo(right.getEdgeId()));
        }
        return result;
    }

    @Override
    public List<GraphEdgeRef> neighbors(String entityId) {
        List<GraphEdgeRef> result = neighbors.get(entityId);
        return result == null ? Collections.emptyList() : Collections.unmodifiableList(result);
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
    public String getDeltaDigest() {
        return deltaDigest;
    }

    @Override
    public String getStatisticsVersion() {
        return statistics.getStatisticsVersion();
    }

    @Override
    public String getFingerprint() {
        return fingerprint;
    }

    @Override
    public GraphStatistics getStatistics() {
        return statistics;
    }

    public GraphSnapshot getSnapshot() {
        return snapshot;
    }

    public List<EntityRef> getEntities() {
        return snapshot.getEntities();
    }

    public List<GraphEdgeRef> getEdges() {
        return snapshot.getEdges();
    }

    public int degree(String entityId) {
        return statistics.getDegree(entityId);
    }

    /** Returns the immutable statistics object; the entity argument is accepted for call-site symmetry. */
    public GraphVertexStatistics statistics(String entityId) {
        return new GraphVertexStatistics(entityId, statistics.getStatisticsVersion(),
            statistics.getDegree(entityId), statistics.getRelationCounts(entityId));
    }

    public GraphVertexStatistics getStatistics(String entityId) {
        return statistics(entityId);
    }

    public GraphQueryLease openLease() {
        return new GraphQueryLease(this);
    }
}
