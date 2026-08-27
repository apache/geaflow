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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.iterator.IMultiPartitionIterator;

/**
 * Multi-partition vertex iterator that merges vertices from multiple partitions
 * in sorted order by vertex ID.
 */
public class MultiPartitionVertexIterator<K, VV> implements IMultiPartitionIterator<IVertex<K, VV>> {

    private final IType<K> keyType;
    private final Map<String, CloseableIterator<IVertex<K, VV>>> partitionIterators;
    private final Map<String, Long> partitionStats;
    private final PriorityQueue<PartitionEntry<K, VV>> heap;
    private boolean closed = false;

    public MultiPartitionVertexIterator(IType<K> keyType) {
        this.keyType = keyType;
        this.partitionIterators = new ConcurrentHashMap<>();
        this.partitionStats = new ConcurrentHashMap<>();
        this.heap = new PriorityQueue<>((a, b) -> keyType.compare(a.vertex.getId(), b.vertex.getId()));
    }

    public MultiPartitionVertexIterator(IType<K> keyType, 
                                       Map<String, CloseableIterator<IVertex<K, VV>>> iterators) {
        this(keyType);
        for (Map.Entry<String, CloseableIterator<IVertex<K, VV>>> entry : iterators.entrySet()) {
            addPartition(entry.getKey(), entry.getValue());
        }
        initializeHeap();
    }

    @Override
    public void addPartition(String partitionName, CloseableIterator<IVertex<K, VV>> iterator) {
        if (closed) {
            throw new IllegalStateException("Iterator is already closed");
        }
        partitionIterators.put(partitionName, iterator);
        partitionStats.put(partitionName, 0L);
    }

    @Override
    public void removePartition(String partitionName) {
        CloseableIterator<IVertex<K, VV>> iterator = partitionIterators.remove(partitionName);
        if (iterator != null) {
            iterator.close();
        }
        partitionStats.remove(partitionName);
        // Remove from heap if present
        heap.removeIf(entry -> entry.partitionName.equals(partitionName));
    }

    @Override
    public List<String> getActivePartitions() {
        return new ArrayList<>(partitionIterators.keySet());
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
        
        // Refill heap if needed
        refillHeap();
        return !heap.isEmpty();
    }

    @Override
    public IVertex<K, VV> next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more elements");
        }

        PartitionEntry<K, VV> entry = heap.poll();
        IVertex<K, VV> vertex = entry.vertex;
        
        // Update statistics
        partitionStats.merge(entry.partitionName, 1L, Long::sum);
        
        // Try to get next vertex from the same partition
        CloseableIterator<IVertex<K, VV>> iterator = partitionIterators.get(entry.partitionName);
        if (iterator != null && iterator.hasNext()) {
            IVertex<K, VV> nextVertex = iterator.next();
            heap.offer(new PartitionEntry<>(entry.partitionName, nextVertex));
        }
        
        return vertex;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        closed = true;
        heap.clear();
        
        for (CloseableIterator<IVertex<K, VV>> iterator : partitionIterators.values()) {
            try {
                iterator.close();
            } catch (Exception e) {
                // Log error but continue closing other iterators
            }
        }
        
        partitionIterators.clear();
        partitionStats.clear();
    }

    private void initializeHeap() {
        for (Map.Entry<String, CloseableIterator<IVertex<K, VV>>> entry : partitionIterators.entrySet()) {
            CloseableIterator<IVertex<K, VV>> iterator = entry.getValue();
            if (iterator.hasNext()) {
                IVertex<K, VV> vertex = iterator.next();
                heap.offer(new PartitionEntry<>(entry.getKey(), vertex));
            }
        }
    }

    private void refillHeap() {
        // This method is called in hasNext() to ensure heap is populated
        if (heap.isEmpty()) {
            initializeHeap();
        }
    }

    private static class PartitionEntry<K, VV> {
        final String partitionName;
        final IVertex<K, VV> vertex;

        PartitionEntry(String partitionName, IVertex<K, VV> vertex) {
            this.partitionName = partitionName;
            this.vertex = vertex;
        }
    }
}
