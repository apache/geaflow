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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.state.partition.IPartitionManager;
import org.apache.geaflow.state.partition.PartitionMetadata;
import org.apache.geaflow.state.pushdown.filter.IFilter;

/**
 * Default implementation of partition scan strategy.
 */
public class DefaultPartitionScanStrategy implements IPartitionScanStrategy {

    private final IPartitionManager partitionManager;
    private final int maxParallelPartitions;

    public DefaultPartitionScanStrategy(IPartitionManager partitionManager) {
        this(partitionManager, 4); // Default to 4 parallel partitions
    }

    public DefaultPartitionScanStrategy(IPartitionManager partitionManager, int maxParallelPartitions) {
        this.partitionManager = partitionManager;
        this.maxParallelPartitions = maxParallelPartitions;
    }

    @Override
    public List<String> optimizeScanOrder(List<String> partitionNames, IFilter filter) {
        // Sort partitions by record count (smaller partitions first for better load balancing)
        return partitionNames.stream()
            .sorted((p1, p2) -> {
                PartitionMetadata meta1 = partitionManager.getPartitionMetadata(p1);
                PartitionMetadata meta2 = partitionManager.getPartitionMetadata(p2);
                
                long count1 = meta1 != null ? meta1.getRecordCount() : 0;
                long count2 = meta2 != null ? meta2.getRecordCount() : 0;
                
                return Long.compare(count1, count2);
            })
            .collect(Collectors.toList());
    }

    @Override
    public boolean shouldUseParallelScanning(List<String> partitionNames) {
        // Use parallel scanning if we have more than one partition
        // and the total estimated records justify the overhead
        if (partitionNames.size() <= 1) {
            return false;
        }
        
        long totalRecords = partitionNames.stream()
            .mapToLong(this::getPartitionRecordCount)
            .sum();
        
        // Use parallel scanning if total records > 1000 (threshold)
        return totalRecords > 1000;
    }

    @Override
    public int getMaxParallelPartitions() {
        return maxParallelPartitions;
    }

    @Override
    public long estimateScanCost(List<String> partitionNames) {
        // Simple cost estimation based on record count
        return partitionNames.stream()
            .mapToLong(this::getPartitionRecordCount)
            .sum();
    }

    private long getPartitionRecordCount(String partitionName) {
        PartitionMetadata metadata = partitionManager.getPartitionMetadata(partitionName);
        return metadata != null ? metadata.getRecordCount() : 0;
    }
}
