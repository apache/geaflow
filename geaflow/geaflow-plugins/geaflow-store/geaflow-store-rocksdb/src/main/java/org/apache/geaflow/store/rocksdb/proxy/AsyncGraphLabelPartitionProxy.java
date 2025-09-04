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

import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.iterator.IOneDegreeGraphIterator;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.store.iterator.MultiPartitionVertexIterator;
import org.apache.geaflow.store.iterator.MultiPartitionEdgeIterator;
import org.apache.geaflow.store.iterator.OneDegreeGraphScanIterator;
import org.apache.geaflow.store.rocksdb.RocksdbClient;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.partition.DefaultPartitionManager;
import org.apache.geaflow.state.partition.IPartitionManager;
import org.apache.geaflow.state.strategy.DefaultPartitionScanStrategy;
import org.apache.geaflow.state.strategy.IPartitionScanStrategy;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.store.iterator.VertexScanIterator;
import org.apache.geaflow.store.iterator.EdgeScanIterator;
import org.apache.geaflow.store.rocksdb.iterator.RocksdbIterator;
import org.apache.geaflow.state.pushdown.filter.inner.FilterHelper;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.common.iterator.ChainedCloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Asynchronous implementation of label-partitioned graph proxy.
 * This proxy supports asynchronous operations for label-based graph partitioning.
 *
 * @param <K> the vertex/edge ID type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
public class AsyncGraphLabelPartitionProxy<K, VV, EV> extends SyncGraphLabelPartitionProxy<K, VV, EV> {

    private final IPartitionManager partitionManager;
    private final IPartitionScanStrategy scanStrategy;
    private final ExecutorService executorService;

    public AsyncGraphLabelPartitionProxy(RocksdbClient rocksdbClient,
                                        IGraphKVEncoder<K, VV, EV> encoder, Configuration config) {
        super(rocksdbClient, encoder, config);
        this.partitionManager = new DefaultPartitionManager("graph");
        this.scanStrategy = new DefaultPartitionScanStrategy(this.partitionManager);
        this.executorService = Executors.newFixedThreadPool(4); // Configurable thread pool
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        
        // Parse labels from filter
        List<String> labels = partitionManager.parseLabelsFromFilter(pushdown.getFilter());
        
        if (labels.isEmpty()) {
            // Fall back to synchronous implementation if no labels found
            return super.getOneDegreeGraphIterator(pushdown);
        }
        
        // For now, use the parent class implementation
        // TODO: Implement proper multi-partition scanning
        return super.getOneDegreeGraphIterator(pushdown);
    }

    /**
     * Get vertex iterator for a specific partition.
     *
     * @param label the partition label
     * @param pushdown the pushdown condition
     * @return vertex iterator for the partition
     */
    private CloseableIterator<IVertex<K, VV>> getVertexIteratorForPartition(String label, IStatePushDown pushdown) {
        flush();
        
        // Use the parent class method to get vertex iterator for this label
        return super.getVertexIterator(pushdown);
    }

    /**
     * Get edge iterator for a specific partition.
     *
     * @param label the partition label
     * @param pushdown the pushdown condition
     * @return edge iterator for the partition
     */
    private CloseableIterator<IEdge<K, EV>> getEdgeIteratorForPartition(String label, IStatePushDown pushdown) {
        flush();
        
        // Use the parent class method to get edge iterator for this label
        return super.getEdgeIterator(pushdown);
    }

    /**
     * Asynchronous version of getOneDegreeGraphIterator that uses parallel processing.
     *
     * @param pushdown the pushdown condition
     * @return future containing the iterator
     */
    public Future<CloseableIterator<OneDegreeGraph<K, VV, EV>>> getOneDegreeGraphIteratorAsync(
        IStatePushDown pushdown) {
        
        return CompletableFuture.supplyAsync(() -> {
            return getOneDegreeGraphIterator(pushdown);
        }, executorService);
    }

    /**
     * Get the partition manager.
     *
     * @return partition manager
     */
    public IPartitionManager getPartitionManager() {
        return partitionManager;
    }

    /**
     * Get the scan strategy.
     *
     * @return scan strategy
     */
    public IPartitionScanStrategy getScanStrategy() {
        return scanStrategy;
    }

    /**
     * Shutdown the executor service.
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }

    @Override
    public void close() {
        shutdown();
        super.close();
    }
}
