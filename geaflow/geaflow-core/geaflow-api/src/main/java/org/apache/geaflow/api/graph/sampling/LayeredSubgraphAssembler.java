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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.state.sampling.LocalNeighborhood;

/**
 * Window-local assembler. It is intentionally not serializable or checkpointed; reusable data is
 * held by each vertex as {@link LocalNeighborhood} instead.
 */
public class LayeredSubgraphAssembler<K, VV, EV> {

    private final Map<K, Assembly<K, VV, EV>> assemblies = new LinkedHashMap<>();
    private final long maxSampledNodes;
    private final long maxSampledEdges;
    private final Comparator<K> idComparator;

    public LayeredSubgraphAssembler() {
        this(SubgraphSamplingSpec.DEFAULT_MAX_SAMPLED_NODES,
            SubgraphSamplingSpec.DEFAULT_MAX_SAMPLED_EDGES, null);
    }

    public LayeredSubgraphAssembler(long maxSampledNodes, long maxSampledEdges,
                                    Comparator<K> idComparator) {
        if (maxSampledNodes < 1 || maxSampledEdges < 1) {
            throw new IllegalArgumentException("sampling limits must be greater than zero");
        }
        this.maxSampledNodes = maxSampledNodes;
        this.maxSampledEdges = maxSampledEdges;
        this.idComparator = idComparator;
    }

    public void start(K rootId, int maxDepth, LocalNeighborhood<K, VV, EV> rootNeighborhood) {
        Objects.requireNonNull(rootId, "rootId");
        Objects.requireNonNull(rootNeighborhood, "rootNeighborhood");
        if (maxDepth < 1) {
            throw new IllegalArgumentException("maxDepth must be greater than zero");
        }
        if (!Objects.equals(rootId, rootNeighborhood.getVertex().getId())) {
            throw new IllegalArgumentException("root neighborhood vertex id does not match rootId");
        }
        if (assemblies.containsKey(rootId)) {
            throw new IllegalStateException("sampling assembly already exists for rootId=" + rootId);
        }
        SampledSubgraph<K, VV, EV> subgraph = new SampledSubgraph<>(rootId,
            rootNeighborhood.getSnapshotVersion(), maxSampledNodes, maxSampledEdges, idComparator);
        subgraph.addNeighborhood(0, rootNeighborhood, maxDepth > 0);
        Assembly<K, VV, EV> assembly = new Assembly<>(maxDepth, subgraph);
        assembly.minDepthByVertex.put(rootId, 0);
        assembly.completedVertices.add(rootId);
        assemblies.put(rootId, assembly);
    }

    public boolean registerRequest(K rootId, K vertexId, int depth) {
        Objects.requireNonNull(vertexId, "vertexId");
        Assembly<K, VV, EV> assembly = requireAssembly(rootId);
        validateDepth(depth, assembly.maxDepth);
        Integer knownDepth = assembly.minDepthByVertex.get(vertexId);
        if (knownDepth != null && knownDepth <= depth) {
            return false;
        }
        if (knownDepth == null && assembly.minDepthByVertex.size() + 1L > maxSampledNodes) {
            throw new SubgraphSamplingLimitException(rootId, "nodes",
                assembly.minDepthByVertex.size() + 1L, maxSampledNodes);
        }
        assembly.minDepthByVertex.put(vertexId, depth);
        assembly.completedVertices.remove(vertexId);
        return true;
    }

    public boolean add(SubgraphSamplingResponse<K, VV, EV> response) {
        Objects.requireNonNull(response, "response");
        Assembly<K, VV, EV> assembly = assemblies.get(response.getRootId());
        if (assembly == null) {
            return false;
        }
        validateDepth(response.getDepth(), assembly.maxDepth);
        K vertexId = response.getNeighborhood().getVertex().getId();
        Integer knownDepth = assembly.minDepthByVertex.get(vertexId);
        if (knownDepth == null) {
            throw new IllegalStateException(String.format(
                "sampling response was not requested, rootId=%s, vertexId=%s",
                response.getRootId(), vertexId));
        }
        if (response.getDepth() > knownDepth || assembly.completedVertices.contains(vertexId)) {
            return false;
        }
        if (response.getDepth() < knownDepth) {
            throw new IllegalStateException(String.format(
                "sampling response depth precedes request, rootId=%s, vertexId=%s, depth=%s",
                response.getRootId(), vertexId, response.getDepth()));
        }
        assembly.subgraph.addNeighborhood(response.getDepth(), response.getNeighborhood(),
            response.getDepth() < assembly.maxDepth);
        assembly.completedVertices.add(vertexId);
        return true;
    }

    public SampledSubgraph<K, VV, EV> take(K rootId) {
        Assembly<K, VV, EV> assembly = assemblies.remove(rootId);
        if (assembly == null) {
            return null;
        }
        Set<K> pending = new LinkedHashSet<>(assembly.minDepthByVertex.keySet());
        pending.removeAll(assembly.completedVertices);
        if (!pending.isEmpty()) {
            throw new IllegalStateException("sampling responses missing for rootId=" + rootId
                + ", vertexIds=" + pending);
        }
        assembly.subgraph.validateComplete();
        return assembly.subgraph;
    }

    public void clear() {
        assemblies.clear();
    }

    private Assembly<K, VV, EV> requireAssembly(K rootId) {
        Assembly<K, VV, EV> assembly = assemblies.get(Objects.requireNonNull(rootId, "rootId"));
        if (assembly == null) {
            throw new IllegalStateException("sampling assembly does not exist for rootId=" + rootId);
        }
        return assembly;
    }

    private void validateDepth(int depth, int maxDepth) {
        if (depth < 1 || depth > maxDepth) {
            throw new IllegalArgumentException("sampling depth is outside configured range: " + depth);
        }
    }

    private static class Assembly<K, VV, EV> {

        private final int maxDepth;
        private final SampledSubgraph<K, VV, EV> subgraph;
        private final Map<K, Integer> minDepthByVertex = new LinkedHashMap<>();
        private final Set<K> completedVertices = new LinkedHashSet<>();

        private Assembly(int maxDepth, SampledSubgraph<K, VV, EV> subgraph) {
            this.maxDepth = maxDepth;
            this.subgraph = subgraph;
        }
    }
}
