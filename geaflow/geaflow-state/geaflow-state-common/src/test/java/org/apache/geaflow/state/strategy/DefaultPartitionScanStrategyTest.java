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

package org.apache.geaflow.state.strategy;

import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.state.partition.DefaultPartitionManager;
import org.apache.geaflow.state.partition.IPartitionManager;
import org.apache.geaflow.state.partition.PartitionMetadata;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPartitionScanStrategyTest {

    @Test
    public void testOptimizeScanOrder() {
        IPartitionManager partitionManager = new DefaultPartitionManager();
        
        // Register partitions with different record counts
        partitionManager.registerPartition("partition_small", 
            new PartitionMetadata("partition_small", "small", 100, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition_medium", 
            new PartitionMetadata("partition_medium", "medium", 500, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition_large", 
            new PartitionMetadata("partition_large", "large", 1000, System.currentTimeMillis(), true));
        
        DefaultPartitionScanStrategy strategy = new DefaultPartitionScanStrategy(partitionManager);
        
        List<String> partitions = Arrays.asList("partition_large", "partition_small", "partition_medium");
        List<String> optimizedOrder = strategy.optimizeScanOrder(partitions, null);
        
        // Should be sorted by record count (smallest first)
        Assert.assertEquals("partition_small", optimizedOrder.get(0));
        Assert.assertEquals("partition_medium", optimizedOrder.get(1));
        Assert.assertEquals("partition_large", optimizedOrder.get(2));
    }

    @Test
    public void testShouldUseParallelScanning() {
        IPartitionManager partitionManager = new DefaultPartitionManager();
        DefaultPartitionScanStrategy strategy = new DefaultPartitionScanStrategy(partitionManager);
        
        // Single partition - should not use parallel scanning
        Assert.assertFalse(strategy.shouldUseParallelScanning(Arrays.asList("partition1")));
        
        // Multiple partitions with low record count - should not use parallel scanning
        partitionManager.registerPartition("partition1", 
            new PartitionMetadata("partition1", "label1", 100, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition2", 
            new PartitionMetadata("partition2", "label2", 200, System.currentTimeMillis(), true));
        
        Assert.assertFalse(strategy.shouldUseParallelScanning(Arrays.asList("partition1", "partition2")));
        
        // Multiple partitions with high record count - should use parallel scanning
        partitionManager.registerPartition("partition3", 
            new PartitionMetadata("partition3", "label3", 1000, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition4", 
            new PartitionMetadata("partition4", "label4", 2000, System.currentTimeMillis(), true));
        
        Assert.assertTrue(strategy.shouldUseParallelScanning(Arrays.asList("partition3", "partition4")));
    }

    @Test
    public void testGetMaxParallelPartitions() {
        IPartitionManager partitionManager = new DefaultPartitionManager();
        
        DefaultPartitionScanStrategy strategy1 = new DefaultPartitionScanStrategy(partitionManager);
        Assert.assertEquals(4, strategy1.getMaxParallelPartitions());
        
        DefaultPartitionScanStrategy strategy2 = new DefaultPartitionScanStrategy(partitionManager, 8);
        Assert.assertEquals(8, strategy2.getMaxParallelPartitions());
    }

    @Test
    public void testEstimateScanCost() {
        IPartitionManager partitionManager = new DefaultPartitionManager();
        
        // Register partitions with different record counts
        partitionManager.registerPartition("partition1", 
            new PartitionMetadata("partition1", "label1", 100, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition2", 
            new PartitionMetadata("partition2", "label2", 200, System.currentTimeMillis(), true));
        partitionManager.registerPartition("partition3", 
            new PartitionMetadata("partition3", "label3", 300, System.currentTimeMillis(), true));
        
        DefaultPartitionScanStrategy strategy = new DefaultPartitionScanStrategy(partitionManager);
        
        List<String> partitions = Arrays.asList("partition1", "partition2", "partition3");
        long estimatedCost = strategy.estimateScanCost(partitions);
        
        // Should be sum of all record counts
        Assert.assertEquals(600L, estimatedCost);
    }
}
