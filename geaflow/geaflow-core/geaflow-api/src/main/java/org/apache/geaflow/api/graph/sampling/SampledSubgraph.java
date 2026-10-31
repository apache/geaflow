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

package org.apache.geaflow.api.graph.sampling;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.sampling.LocalNeighborhood;

/**
 * Short-lived layered view assembled from reusable one-hop vertex state.
 */
public class SampledSubgraph<K, VV, EV> implements Serializable {

    private final K rootId;
    private final long snapshotVersion;
    private final Map<K, IVertex<K, VV>> vertices = new LinkedHashMap<>();
    private final List<List<IEdge<K, EV>>> edgeLayers = new ArrayList<>();
    private final Set<LogicalEdgeId<K>> edgeIdentities = new LinkedHashSet<>();
    private final long maxSampledNodes;
    private final long maxSampledEdges;
    private final transient Comparator<K> idComparator;

    public SampledSubgraph(K rootId, long snapshotVersion) {
        this(rootId, snapshotVersion, SubgraphSamplingSpec.DEFAULT_MAX_SAMPLED_NODES,
            SubgraphSamplingSpec.DEFAULT_MAX_SAMPLED_EDGES, null);
    }

    public SampledSubgraph(K rootId, long snapshotVersion, long maxSampledNodes,
                           long maxSampledEdges, Comparator<K> idComparator) {
        this.rootId = Objects.requireNonNull(rootId, "rootId");
        this.snapshotVersion = snapshotVersion;
        this.maxSampledNodes = maxSampledNodes;
        this.maxSampledEdges = maxSampledEdges;
        this.idComparator = idComparator;
    }

    public K getRootId() {
        return rootId;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    public void addVertex(IVertex<K, VV> vertex) {
        Objects.requireNonNull(vertex, "vertex");
        K vertexId = Objects.requireNonNull(vertex.getId(), "vertexId");
        if (!vertices.containsKey(vertexId) && vertices.size() + 1L > maxSampledNodes) {
            throw new SubgraphSamplingLimitException(rootId, "nodes",
                vertices.size() + 1L, maxSampledNodes);
        }
        vertices.put(vertexId, vertex);
    }

    public void addNeighborhood(int depth, LocalNeighborhood<K, VV, EV> neighborhood,
                                boolean includeEdges) {
        if (depth < 0) {
            throw new IllegalArgumentException("sampling neighborhood depth must not be negative");
        }
        Objects.requireNonNull(neighborhood, "neighborhood");
        if (neighborhood.getSnapshotVersion() != snapshotVersion) {
            throw new IllegalArgumentException("neighborhood snapshot does not match assembly snapshot");
        }
        addVertex(neighborhood.getVertex());
        if (!includeEdges) {
            return;
        }
        while (edgeLayers.size() <= depth) {
            edgeLayers.add(new ArrayList<>());
        }
        List<IEdge<K, EV>> layer = edgeLayers.get(depth);
        for (IEdge<K, EV> edge : neighborhood.getEdges()) {
            addEdge(layer, edge);
        }
    }

    public Map<K, IVertex<K, VV>> getVertices() {
        if (idComparator == null || vertices.size() < 2) {
            return Collections.unmodifiableMap(vertices);
        }
        List<K> ids = new ArrayList<>(vertices.keySet());
        ids.sort((left, right) -> {
            if (Objects.equals(left, rootId)) {
                return Objects.equals(right, rootId) ? 0 : -1;
            }
            if (Objects.equals(right, rootId)) {
                return 1;
            }
            return idComparator.compare(left, right);
        });
        Map<K, IVertex<K, VV>> ordered = new LinkedHashMap<>();
        for (K id : ids) {
            ordered.put(id, vertices.get(id));
        }
        return Collections.unmodifiableMap(ordered);
    }

    public List<List<IEdge<K, EV>>> getEdgeLayers() {
        List<List<IEdge<K, EV>>> layers = new ArrayList<>(edgeLayers.size());
        for (List<IEdge<K, EV>> layer : edgeLayers) {
            List<IEdge<K, EV>> ordered = new ArrayList<>(layer);
            if (idComparator != null) {
                ordered.sort(this::compareEdges);
            }
            layers.add(Collections.unmodifiableList(ordered));
        }
        return Collections.unmodifiableList(layers);
    }

    public void validateComplete() {
        for (List<IEdge<K, EV>> layer : edgeLayers) {
            for (IEdge<K, EV> edge : layer) {
                if (!vertices.containsKey(edge.getSrcId()) || !vertices.containsKey(edge.getTargetId())) {
                    throw new IllegalStateException(String.format(
                        "sampled subgraph contains dangling edge, rootId=%s, srcId=%s, targetId=%s",
                        rootId, edge.getSrcId(), edge.getTargetId()));
                }
            }
        }
    }

    private void addEdge(List<IEdge<K, EV>> layer, IEdge<K, EV> edge) {
        Objects.requireNonNull(edge, "edge");
        Objects.requireNonNull(edge.getSrcId(), "edge.srcId");
        Objects.requireNonNull(edge.getTargetId(), "edge.targetId");
        // Incoming storage copies and outgoing copies represent the same logical edge after normalization.
        LogicalEdgeId<K> identity = LogicalEdgeId.fromNormalized(edge);
        if (edgeIdentities.add(identity)) {
            if (edgeIdentities.size() > maxSampledEdges) {
                edgeIdentities.remove(identity);
                throw new SubgraphSamplingLimitException(rootId, "edges",
                    edgeIdentities.size() + 1L, maxSampledEdges);
            }
            layer.add(edge);
        }
    }

    private int compareEdges(IEdge<K, EV> left, IEdge<K, EV> right) {
        int result = idComparator.compare(left.getSrcId(), right.getSrcId());
        if (result == 0) {
            result = idComparator.compare(left.getTargetId(), right.getTargetId());
        }
        if (result == 0) {
            result = String.valueOf(labelOf(left)).compareTo(String.valueOf(labelOf(right)));
        }
        if (result == 0) {
            result = String.valueOf(timeOf(left)).compareTo(String.valueOf(timeOf(right)));
        }
        if (result == 0) {
            result = String.valueOf(left.getValue()).compareTo(String.valueOf(right.getValue()));
        }
        return result;
    }

    private static String labelOf(IEdge<?, ?> edge) {
        return edge instanceof IGraphElementWithLabelField
            ? ((IGraphElementWithLabelField) edge).getLabel() : null;
    }

    private static Long timeOf(IEdge<?, ?> edge) {
        return edge instanceof IGraphElementWithTimeField
            ? ((IGraphElementWithTimeField) edge).getTime() : null;
    }

}
