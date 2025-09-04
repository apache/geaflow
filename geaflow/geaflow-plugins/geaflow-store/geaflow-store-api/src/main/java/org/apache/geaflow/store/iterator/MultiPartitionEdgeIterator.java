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

import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.iterator.IMultiPartitionIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Iterator for merging edges from multiple partitions in sorted order.
 * This iterator merges edges from different partitions based on their source vertex IDs.
 *
 * @param <K> the edge ID type
 * @param <EV> the edge value type
 */
public class MultiPartitionEdgeIterator<K, EV> implements IMultiPartitionIterator<IEdge<K, EV>> {

    private final Map<String, CloseableIterator<IEdge<K, EV>>> partitionIterators;
    private final PriorityQueue<EdgeEntry<K, EV>> edgeQueue;
    private final Map<String, Long> partitionStats;
    private boolean closed = false;

    public MultiPartitionEdgeIterator() {
        this.partitionIterators = new HashMap<>();
        this.edgeQueue = new PriorityQueue<>();
        this.partitionStats = new HashMap<>();
    }

    @Override
    public List<String> getActivePartitions() {
        return new ArrayList<>(partitionIterators.keySet());
    }

    @Override
    public void addPartition(String partitionName, CloseableIterator<IEdge<K, EV>> iterator) {
        partitionIterators.put(partitionName, iterator);
        partitionStats.put(partitionName, 0L);
        // Add the first edge from this partition to the queue
        if (iterator.hasNext()) {
            IEdge<K, EV> edge = iterator.next();
            edgeQueue.offer(new EdgeEntry<>(partitionName, edge));
            partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
        }
    }

    @Override
    public void removePartition(String partitionName) {
        CloseableIterator<IEdge<K, EV>> iterator = partitionIterators.remove(partitionName);
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
        while (edgeQueue.isEmpty() && !partitionIterators.isEmpty()) {
            boolean hasMoreData = false;
            for (Map.Entry<String, CloseableIterator<IEdge<K, EV>>> entry : partitionIterators.entrySet()) {
                String partitionName = entry.getKey();
                CloseableIterator<IEdge<K, EV>> iterator = entry.getValue();
                
                if (iterator.hasNext()) {
                    IEdge<K, EV> edge = iterator.next();
                    edgeQueue.offer(new EdgeEntry<>(partitionName, edge));
                    partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
                    hasMoreData = true;
                }
            }
            
            if (!hasMoreData) {
                break;
            }
        }
        
        return !edgeQueue.isEmpty();
    }

    @Override
    public IEdge<K, EV> next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException();
        }
        
        EdgeEntry<K, EV> entry = edgeQueue.poll();
        String partitionName = entry.partitionName;
        IEdge<K, EV> edge = entry.edge;
        
        // Add the next edge from this partition to the queue
        CloseableIterator<IEdge<K, EV>> iterator = partitionIterators.get(partitionName);
        if (iterator.hasNext()) {
            IEdge<K, EV> nextEdge = iterator.next();
            edgeQueue.offer(new EdgeEntry<>(partitionName, nextEdge));
            partitionStats.put(partitionName, partitionStats.get(partitionName) + 1);
        }
        
        return edge;
    }

    @Override
    public void close() {
        if (!closed) {
            for (CloseableIterator<IEdge<K, EV>> iterator : partitionIterators.values()) {
                iterator.close();
            }
            partitionIterators.clear();
            edgeQueue.clear();
            partitionStats.clear();
            closed = true;
        }
    }

    /**
     * Helper class to hold edge and partition information for priority queue.
     */
    private static class EdgeEntry<K, EV> implements Comparable<EdgeEntry<K, EV>> {
        final String partitionName;
        final IEdge<K, EV> edge;

        EdgeEntry(String partitionName, IEdge<K, EV> edge) {
            this.partitionName = partitionName;
            this.edge = edge;
        }

        @Override
        public int compareTo(EdgeEntry<K, EV> other) {
            // Compare by source vertex ID
            @SuppressWarnings("unchecked")
            Comparable<K> thisSrcId = (Comparable<K>) edge.getSrcId();
            @SuppressWarnings("unchecked")
            Comparable<K> otherSrcId = (Comparable<K>) other.edge.getSrcId();
            
            if (thisSrcId == null || otherSrcId == null) {
                return 0; // Consider them equal if IDs are not comparable
            }
            
            return thisSrcId.compareTo(other.edge.getSrcId());
        }
    }
}
