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

import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.iterator.IMultiPartitionIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Iterator for merging vertices from multiple partitions in sorted order.
 * This iterator merges vertices from different partitions based on their vertex IDs.
 *
 * @param <K> the vertex ID type
 * @param <VV> the vertex value type
 */
public class MultiPartitionVertexIterator<K, VV> implements IMultiPartitionIterator<IVertex<K, VV>> {

    private final Map<String, CloseableIterator<IVertex<K, VV>>> partitionIterators;
    private final PriorityQueue<VertexEntry<K, VV>> vertexQueue;
    private final Map<String, Long> partitionStats;
    private boolean closed = false;

    public MultiPartitionVertexIterator() {
        this.partitionIterators = new HashMap<>();
        this.vertexQueue = new PriorityQueue<>();
        this.partitionStats = new HashMap<>();
    }

    @Override
    public List<String> getActivePartitions() {
        return new ArrayList<>(partitionIterators.keySet());
    }

    @Override
    public void addPartition(String partitionName, CloseableIterator<IVertex<K, VV>> iterator) {
        partitionIterators.put(partitionName, iterator);
        partitionStats.put(partitionName, 0L);
        // Add the first vertex from this partition to the queue
        if (iterator.hasNext()) {
            IVertex<K, VV> vertex = iterator.next();
            vertexQueue.offer(new VertexEntry<>(partitionName, vertex));
            partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
        }
    }

    @Override
    public void removePartition(String partitionName) {
        CloseableIterator<IVertex<K, VV>> iterator = partitionIterators.remove(partitionName);
        if (iterator != null) {
            iterator.close();
        }
        partitionStats.remove(partitionName);
    }

    @Override
    public Map<String, Long> getPartitionStats() {
        return new HashMap<>(partitionStats);
    }

    @Override
    public boolean isPartitionActive(String partitionName) {
        return partitionIterators.containsKey(partitionName);
    }

    @Override
    public int getPartitionCount() {
        return partitionIterators.size();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        
        // Refill the queue if it's empty
        while (vertexQueue.isEmpty() && !partitionIterators.isEmpty()) {
            boolean hasMoreData = false;
            for (Map.Entry<String, CloseableIterator<IVertex<K, VV>>> entry : partitionIterators.entrySet()) {
                String partitionName = entry.getKey();
                CloseableIterator<IVertex<K, VV>> iterator = entry.getValue();
                
                if (iterator.hasNext()) {
                    IVertex<K, VV> vertex = iterator.next();
                    vertexQueue.offer(new VertexEntry<>(partitionName, vertex));
                    partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
                    hasMoreData = true;
                }
            }
            
            if (!hasMoreData) {
                break;
            }
        }
        
        return !vertexQueue.isEmpty();
    }

    @Override
    public IVertex<K, VV> next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException();
        }
        
        VertexEntry<K, VV> entry = vertexQueue.poll();
        String partitionName = entry.partitionName;
        IVertex<K, VV> vertex = entry.vertex;
        
        // Add the next vertex from this partition to the queue
        CloseableIterator<IVertex<K, VV>> iterator = partitionIterators.get(partitionName);
        if (iterator.hasNext()) {
            IVertex<K, VV> nextVertex = iterator.next();
            vertexQueue.offer(new VertexEntry<>(partitionName, nextVertex));
            partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
        }
        
        return vertex;
    }

    @Override
    public void close() {
        if (!closed) {
            for (CloseableIterator<IVertex<K, VV>> iterator : partitionIterators.values()) {
                iterator.close();
            }
            partitionIterators.clear();
            vertexQueue.clear();
            partitionStats.clear();
            closed = true;
        }
    }

    /**
     * Helper class to hold vertex and partition information for priority queue.
     */
    private static class VertexEntry<K, VV> implements Comparable<VertexEntry<K, VV>> {
        final String partitionName;
        final IVertex<K, VV> vertex;

        VertexEntry(String partitionName, IVertex<K, VV> vertex) {
            this.partitionName = partitionName;
            this.vertex = vertex;
        }

        @Override
        public int compareTo(VertexEntry<K, VV> other) {
            // Compare by vertex ID
            @SuppressWarnings("unchecked")
            Comparable<K> thisId = (Comparable<K>) vertex.getId();
            @SuppressWarnings("unchecked")
            Comparable<K> otherId = (Comparable<K>) other.vertex.getId();
            
            if (thisId == null || otherId == null) {
                return 0; // Consider them equal if IDs are not comparable
            }
            
            return thisId.compareTo(other.vertex.getId());
        }
    }
}
