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

package org.apache.geaflow.store.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.iterator.IOneDegreeGraphIterator;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;

// Enhanced one degree graph scan iterator with partition-aware support
public class OneDegreeGraphScanIterator<K, VV, EV> implements
    IOneDegreeGraphIterator<K, VV, EV> {

    private final CloseableIterator<IVertex<K, VV>> vertexIterator;
    private final CloseableIterator<List<IEdge<K, EV>>> edgeListIterator;
    private final IType<K> keyType;
    private IGraphFilter filter;
    private OneDegreeGraph<K, VV, EV> nextValue;

    private List<IEdge<K, EV>> candidateEdges;
    private IVertex<K, VV> candidateVertex;
    
    // Partition-aware fields
    private boolean partitionAware = false;
    private org.apache.geaflow.state.strategy.IPartitionScanStrategy scanStrategy;

    public OneDegreeGraphScanIterator(
        IType<K> keyType,
        CloseableIterator<IVertex<K, VV>> vertexIterator,
        CloseableIterator<IEdge<K, EV>> edgeIterator,
        IStatePushDown pushdown) {
        this.keyType = keyType;
        this.vertexIterator = vertexIterator;
        this.edgeListIterator = new EdgeListScanIterator<>(edgeIterator);
        this.filter = (IGraphFilter) pushdown.getFilter();
    }

    /**
     * Partition-aware constructor for multi-partition scanning.
     */
    public OneDegreeGraphScanIterator(
        IType<K> keyType,
        org.apache.geaflow.state.iterator.IMultiPartitionIterator<IVertex<K, VV>> vertexIterator,
        org.apache.geaflow.state.iterator.IMultiPartitionIterator<IEdge<K, EV>> edgeIterator,
        IStatePushDown pushdown) {
        
        this.keyType = keyType;
        this.vertexIterator = vertexIterator;
        this.edgeListIterator = new EdgeListScanIterator<>(edgeIterator);
        this.filter = (IGraphFilter) pushdown.getFilter();
        this.partitionAware = true;
    }

    /**
     * Partition-aware constructor with scan strategy.
     */
    public OneDegreeGraphScanIterator(
        IType<K> keyType,
        org.apache.geaflow.state.iterator.IMultiPartitionIterator<IVertex<K, VV>> vertexIterator,
        org.apache.geaflow.state.iterator.IMultiPartitionIterator<IEdge<K, EV>> edgeIterator,
        IStatePushDown pushdown,
        org.apache.geaflow.state.strategy.IPartitionScanStrategy scanStrategy) {
        
        this.keyType = keyType;
        this.vertexIterator = vertexIterator;
        this.edgeListIterator = new EdgeListScanIterator<>(edgeIterator);
        this.filter = (IGraphFilter) pushdown.getFilter();
        this.partitionAware = true;
        this.scanStrategy = scanStrategy;
    }


    private IVertex<K, VV> getVertexFromIterator() {
        if (vertexIterator.hasNext()) {
            return vertexIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        do {
            candidateVertex = candidateVertex == null ? getVertexFromIterator() : candidateVertex;
            if (candidateEdges == null && edgeListIterator.hasNext()) {
                candidateEdges = edgeListIterator.next();
            } else if (candidateEdges == null) {
                candidateEdges = new ArrayList<>();
            }

            if (candidateEdges.size() > 0 && candidateVertex != null) {
                K edgeKey = candidateEdges.get(0).getSrcId();
                K vertexKey = candidateVertex.getId();
                int res = keyType.compare(edgeKey, vertexKey);
                if (res < 0) {
                    nextValue = mergeFromPartitions(edgeKey, null, candidateEdges);
                    candidateEdges = null;
                } else if (res == 0) {
                    nextValue = mergeFromPartitions(vertexKey, candidateVertex, candidateEdges);
                    candidateVertex = null;
                    candidateEdges = null;
                } else {
                    nextValue = mergeFromPartitions(vertexKey, candidateVertex, new ArrayList<>());
                    candidateVertex = null;
                }
            } else if (candidateEdges.size() > 0) {
                nextValue = mergeFromPartitions(candidateEdges.get(0).getSrcId(), null, candidateEdges);
                candidateEdges = null;
            } else if (candidateVertex != null) {
                nextValue = mergeFromPartitions(candidateVertex.getId(), candidateVertex, new ArrayList<>());
                candidateVertex = null;
            } else {
                return false;
            }

            if (!filter.filterOneDegreeGraph(nextValue)) {
                continue;
            }
            return true;
        } while (true);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> next() {
        return nextValue;
    }

    @Override
    public List<String> getActivePartitions() {
        if (!partitionAware) {
            return java.util.Collections.emptyList();
        }
        
        java.util.Set<String> allPartitions = new java.util.HashSet<>();
        if (vertexIterator instanceof org.apache.geaflow.state.iterator.IMultiPartitionIterator) {
            allPartitions.addAll(((org.apache.geaflow.state.iterator.IMultiPartitionIterator<?>) vertexIterator).getActivePartitions());
        }
        if (edgeListIterator instanceof org.apache.geaflow.state.iterator.IMultiPartitionIterator) {
            allPartitions.addAll(((org.apache.geaflow.state.iterator.IMultiPartitionIterator<?>) edgeListIterator).getActivePartitions());
        }
        return new java.util.ArrayList<>(allPartitions);
    }

    @Override
    public boolean isPartitionAware() {
        return partitionAware;
    }

    @Override
    public java.util.Map<String, Long> getPartitionStats() {
        if (!partitionAware) {
            return java.util.Collections.emptyMap();
        }
        
        java.util.Map<String, Long> stats = new java.util.HashMap<>();
        if (vertexIterator instanceof org.apache.geaflow.state.iterator.IMultiPartitionIterator) {
            java.util.Map<String, Long> vertexStats = ((org.apache.geaflow.state.iterator.IMultiPartitionIterator<?>) vertexIterator).getPartitionStats();
            vertexStats.forEach((partition, count) -> 
                stats.merge(partition + "_vertices", count, Long::sum));
        }
        if (edgeListIterator instanceof org.apache.geaflow.state.iterator.IMultiPartitionIterator) {
            java.util.Map<String, Long> edgeStats = ((org.apache.geaflow.state.iterator.IMultiPartitionIterator<?>) edgeListIterator).getPartitionStats();
            edgeStats.forEach((partition, count) -> 
                stats.merge(partition + "_edges", count, Long::sum));
        }
        return stats;
    }

    /**
     * Enhanced merge logic for cross-partition data.
     */
    private OneDegreeGraph<K, VV, EV> mergeFromPartitions(K vertexId, IVertex<K, VV> vertex, List<IEdge<K, EV>> edges) {
        if (!partitionAware) {
            // Use existing merge logic for non-partitioned case
            return new OneDegreeGraph<K, VV, EV>(vertexId, vertex, 
                org.apache.geaflow.state.iterator.IteratorWithClose.wrap(edges.iterator()));
        }
        
        // Enhanced merge logic for partition-aware case
        // Ensure data consistency across partitions
        List<IEdge<K, EV>> validEdges = edges.stream()
            .filter(edge -> edge.getSrcId().equals(vertexId))
            .collect(java.util.stream.Collectors.toList());
        
        return new OneDegreeGraph<K, VV, EV>(vertexId, vertex, 
            org.apache.geaflow.state.iterator.IteratorWithClose.wrap(validEdges.iterator()));
    }

    @Override
    public void close() {
        this.vertexIterator.close();
        this.edgeListIterator.close();
    }
}
