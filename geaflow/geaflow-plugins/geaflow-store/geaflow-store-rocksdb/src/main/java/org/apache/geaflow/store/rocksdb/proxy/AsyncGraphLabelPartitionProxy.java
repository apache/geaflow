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

package org.apache.geaflow.store.rocksdb.proxy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.store.rocksdb.RocksdbClient;

/**
 * Asynchronous implementation of label-partitioned graph proxy.
 * This class provides asynchronous operations for label-partitioned graph data.
 */
public class AsyncGraphLabelPartitionProxy<K, VV, EV> extends SyncGraphLabelPartitionProxy<K, VV, EV> {

    private final ExecutorService executorService;
    private final Map<String, CompletableFuture<Void>> pendingWrites;

    public AsyncGraphLabelPartitionProxy(RocksdbClient rocksdbClient, 
                                        IGraphKVEncoder<K, VV, EV> encoder, 
                                        Configuration config) {
        super(rocksdbClient, encoder, config);
        this.executorService = Executors.newFixedThreadPool(
            config.getInteger("geaflow.store.async.thread.pool.size", 4));
        this.pendingWrites = new ConcurrentHashMap<>();
    }

    @Override
    public void addVertex(K sid, IVertex<K, VV> vertex) {
        String label = vertex.getLabel();
        String cfName = getColumnFamilyName(label, true);
        
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            super.addVertex(sid, vertex);
        }, executorService);
        
        pendingWrites.put(cfName + "_" + sid.toString(), future);
    }

    @Override
    public void addEdge(K sid, IEdge<K, EV> edge) {
        String label = edge.getLabel();
        String cfName = getColumnFamilyName(label, false);
        
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            super.addEdge(sid, edge);
        }, executorService);
        
        pendingWrites.put(cfName + "_" + sid.toString(), future);
    }

    @Override
    public void flush() {
        // Wait for all pending writes to complete
        CompletableFuture.allOf(pendingWrites.values().toArray(new CompletableFuture[0])).join();
        pendingWrites.clear();
        super.flush();
    }

    @Override
    public void close() {
        // Wait for all pending writes before closing
        flush();
        executorService.shutdown();
        super.close();
    }

    /**
     * Get the number of pending writes.
     */
    public int getPendingWriteCount() {
        return pendingWrites.size();
    }

    /**
     * Check if there are any pending writes.
     */
    public boolean hasPendingWrites() {
        return !pendingWrites.isEmpty();
    }
}
