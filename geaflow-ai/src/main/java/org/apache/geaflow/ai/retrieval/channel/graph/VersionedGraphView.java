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
import java.util.Objects;
import java.util.Set;
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
    private final VersionedGraphView parent;
    private final Map<String, List<GraphEdgeRef>> neighbors;
    private final Map<String, GraphVertexRef> vertices;
    private final Set<String> removedVertexKeys;
    private final Map<String, GraphEdgeRef> edgesById;
    private final Set<String> removedEdgeKeys;
    private final String fingerprint;

    private VersionedGraphView(GraphSnapshot snapshot, List<GraphDelta> deltas) {
        this.snapshot = GraphDelta.applyChainTo(snapshot, deltas);
        StringBuilder digest = new StringBuilder();
        for (GraphDelta delta : deltas) {
            digest.append(delta.getDigest()).append('\n');
        }
        this.graphVersion = this.snapshot.getGraphVersion();
        this.deltaCanonical = digest.toString();
        this.deltaDigest = deltaCanonical.isEmpty() ? "" : GraphSnapshot.sha256(deltaCanonical);
        this.statistics = this.snapshot.getStatistics();
        this.parent = null;
        this.neighbors = buildNeighbors(this.snapshot.getEdges());
        this.vertices = new HashMap<>();
        this.edgesById = indexEdges(this.snapshot.getEdges());
        this.removedVertexKeys = Collections.emptySet();
        this.removedEdgeKeys = Collections.emptySet();
        for (GraphVertexRef vertex : this.snapshot.getVertices()) {
            vertices.put(vertex.getEntityId(), vertex);
        }
        this.fingerprint = GraphSnapshot.sha256(this.snapshot.getFingerprint() + "\n" + deltaDigest
            + "\n" + statistics.getStatisticsVersion());
    }

    private VersionedGraphView(VersionedGraphView parent, GraphDelta delta) {
        this.snapshot = delta.applyTo(parent.snapshot);
        this.graphVersion = snapshot.getGraphVersion();
        this.deltaCanonical = parent.deltaCanonical + delta.getDigest() + '\n';
        this.deltaDigest = GraphSnapshot.sha256(deltaCanonical);
        this.statistics = overlayStatistics(parent, delta);
        this.parent = parent;
        this.neighbors = overlayNeighbors(parent, delta);
        this.vertices = overlayVertices(delta);
        this.removedVertexKeys = removedVertexKeys(parent, delta);
        this.edgesById = overlayEdges(delta);
        this.removedEdgeKeys = removedEdgeKeys(parent, delta);
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

    private static Map<String, GraphEdgeRef> indexEdges(List<GraphEdgeRef> edges) {
        Map<String, GraphEdgeRef> result = new HashMap<>();
        for (GraphEdgeRef edge : edges) {
            result.put(edge.getEdgeId(), edge);
        }
        return result;
    }

    private static Map<String, List<GraphEdgeRef>> overlayNeighbors(VersionedGraphView parent,
                                                                     GraphDelta delta) {
        Map<String, List<GraphEdgeRef>> result = new HashMap<>();
        Set<String> changedVertices = new HashSet<>();
        Set<String> removedEdges = new HashSet<>(delta.getTombstones().getEdges());
        for (String edgeId : delta.getTombstones().getEdges()) {
            addEndpoints(parent.edgeById(edgeId), changedVertices);
        }
        for (String entityId : delta.getTombstones().getEntities()) {
            changedVertices.add(entityId);
            for (GraphEdgeRef edge : parent.neighbors(entityId)) {
                removedEdges.add(edge.getEdgeId());
                addEndpoints(edge, changedVertices);
            }
        }
        for (GraphEdgeRef edge : delta.getEdges()) {
            GraphEdgeRef previous = parent.edgeById(edge.getEdgeId());
            addEndpoints(previous, changedVertices);
            addEndpoints(edge, changedVertices);
            removedEdges.add(edge.getEdgeId());
        }
        for (String entityId : changedVertices) {
            List<GraphEdgeRef> updated = new ArrayList<>();
            for (GraphEdgeRef edge : parent.neighbors(entityId)) {
                if (!removedEdges.contains(edge.getEdgeId())
                    && !delta.getTombstones().getEntities().contains(edge.getSourceEntityId())
                    && !delta.getTombstones().getEntities().contains(edge.getTargetEntityId())) {
                    updated.add(edge);
                }
            }
            for (GraphEdgeRef edge : delta.getEdges()) {
                if (edge.getSourceEntityId().equals(entityId) || edge.getTargetEntityId().equals(entityId)) {
                    updated.add(edge);
                }
            }
            updated.sort((left, right) -> left.getEdgeId().compareTo(right.getEdgeId()));
            if (updated.isEmpty()) {
                result.put(entityId, Collections.emptyList());
            } else {
                result.put(entityId, Collections.unmodifiableList(updated));
            }
        }
        return result;
    }

    private static GraphStatistics overlayStatistics(VersionedGraphView parent, GraphDelta delta) {
        Map<String, GraphEdgeRef> removed = new HashMap<>();
        for (String edgeId : delta.getTombstones().getEdges()) {
            addRemoved(parent.edgeById(edgeId), removed);
        }
        for (String entityId : delta.getTombstones().getEntities()) {
            for (GraphEdgeRef edge : parent.neighbors(entityId)) {
                addRemoved(edge, removed);
            }
        }
        for (GraphEdgeRef edge : delta.getEdges()) {
            addRemoved(parent.edgeById(edge.getEdgeId()), removed);
        }
        return GraphStatistics.apply(parent.statistics, removed.values(), delta.getEdges());
    }

    private static void addRemoved(GraphEdgeRef edge, Map<String, GraphEdgeRef> removed) {
        if (edge != null) {
            removed.put(edge.getEdgeId(), edge);
        }
    }

    private static void addEndpoints(GraphEdgeRef edge, Set<String> values) {
        if (edge != null) {
            values.add(edge.getSourceEntityId());
            values.add(edge.getTargetEntityId());
        }
    }

    private static Map<String, GraphVertexRef> overlayVertices(GraphDelta delta) {
        Map<String, GraphVertexRef> result = new HashMap<>();
        for (GraphVertexRef vertex : delta.getVertices()) {
            result.put(vertex.getEntityId(), vertex);
        }
        return result;
    }

    private static Set<String> removedVertexKeys(VersionedGraphView parent, GraphDelta delta) {
        Set<String> result = new HashSet<>(delta.getTombstones().getEntities());
        Set<String> removedIds = new HashSet<>(delta.getTombstones().getVertices());
        if (delta.getVertices().isEmpty() && removedIds.isEmpty()) {
            return result;
        }
        Map<String, String> replacementOwners = new HashMap<>();
        for (GraphVertexRef vertex : delta.getVertices()) {
            replacementOwners.put(vertex.getVertexId(), vertex.getEntityId());
        }
        for (GraphVertexRef vertex : parent.snapshot.getVertices()) {
            String replacementOwner = replacementOwners.get(vertex.getVertexId());
            if (removedIds.contains(vertex.getVertexId())
                || replacementOwner != null && !replacementOwner.equals(vertex.getEntityId())) {
                result.add(vertex.getEntityId());
            }
        }
        return result;
    }

    private static Set<String> removedEdgeKeys(VersionedGraphView parent, GraphDelta delta) {
        Set<String> result = new HashSet<>(delta.getTombstones().getEdges());
        for (String entityId : delta.getTombstones().getEntities()) {
            for (GraphEdgeRef edge : parent.neighbors(entityId)) {
                result.add(edge.getEdgeId());
            }
        }
        for (GraphEdgeRef edge : delta.getEdges()) {
            result.add(edge.getEdgeId());
        }
        return result;
    }

    private static Map<String, GraphEdgeRef> overlayEdges(GraphDelta delta) {
        Map<String, GraphEdgeRef> result = new HashMap<>();
        for (GraphEdgeRef edge : delta.getEdges()) {
            result.put(edge.getEdgeId(), edge);
        }
        return result;
    }

    @Override
    public List<GraphEdgeRef> neighbors(String entityId) {
        List<GraphEdgeRef> result = neighbors.get(entityId);
        if (result == null && parent != null) {
            return parent.neighbors(entityId);
        }
        return result == null ? Collections.emptyList() : Collections.unmodifiableList(result);
    }

    @Override
    public List<GraphEdgeRef> prioritizedNeighbors(String entityId,
                                                   java.util.Comparator<GraphEdgeRef> priority,
                                                   int limit) {
        List<GraphEdgeRef> result = new ArrayList<>(neighbors(entityId));
        result.sort(priority);
        return limit < result.size() ? result.subList(0, limit) : result;
    }

    @Override
    public NeighborBatch prioritizedNeighbors(String entityId,
                                               java.util.Comparator<GraphEdgeRef> priority,
                                               int candidateLimit, int scanLimit) {
        return NeighborBatch.select(neighbors(entityId), priority, candidateLimit, scanLimit);
    }

    @Override
    public GraphVertexRef vertex(EntityRef entity) {
        GraphVertexRef vertex = vertices.get(entity.getEntityId());
        if (vertex == null && parent != null && !removedVertexKeys.contains(entity.getEntityId())) {
            return parent.vertex(entity);
        }
        return vertex == null ? GraphNeighborProvider.super.vertex(entity) : vertex;
    }

    private GraphEdgeRef edgeById(String edgeId) {
        GraphEdgeRef edge = edgesById.get(edgeId);
        if (edge != null || parent == null || removedEdgeKeys.contains(edgeId)) {
            return edge;
        }
        return parent.edgeById(edgeId);
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
