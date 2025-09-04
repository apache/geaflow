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
import org.apache.geaflow.store.rocksdb.StaticGraphRocksdbStoreBase;
import org.apache.geaflow.state.partition.DefaultPartitionManager;
import org.apache.geaflow.state.partition.IPartitionManager;
import org.apache.geaflow.state.strategy.DefaultPartitionScanStrategy;
import org.apache.geaflow.state.strategy.IPartitionScanStrategy;
import org.apache.geaflow.common.type.IType;

import java.util.List;
import java.util.Map;

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

    public AsyncGraphLabelPartitionProxy(RocksdbGraphStore<K, VV, EV> store) {
        super(store);
        this.partitionManager = new DefaultPartitionManager(store.getGraphName());
        this.scanStrategy = new DefaultPartitionScanStrategy();
    }

    @Override
    public IOneDegreeGraphIterator<K, VV, EV> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        
        // Parse labels from filter
        List<String> labels = partitionManager.parseLabelsFromFilter(pushdown.getFilter());
        
        if (labels.isEmpty()) {
            // Fall back to synchronous implementation if no labels found
            return super.getOneDegreeGraphIterator(pushdown);
        }
        
        // Create multi-partition iterators
        MultiPartitionVertexIterator<K, VV> vertexIterator = new MultiPartitionVertexIterator<>();
        MultiPartitionEdgeIterator<K, EV> edgeIterator = new MultiPartitionEdgeIterator<>();
        
        // Add partitions for each label
        for (String label : labels) {
            String partitionName = partitionManager.generatePartitionName(label);
            
            // Get vertex iterator for this partition
            IOneDegreeGraphIterator<K, VV, EV> partitionIterator = 
                super.getOneDegreeGraphIterator(pushdown);
            
            // Add to multi-partition iterator
            vertexIterator.addPartition(partitionName, partitionIterator);
        }
        
        // Create OneDegreeGraphScanIterator with partition awareness
        return new OneDegreeGraphScanIterator<>(
            store.getKeyType(),
            vertexIterator,
            edgeIterator,
            pushdown,
            scanStrategy
        );
    }

    @Override
    public boolean isAsync() {
        return true;
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
}
