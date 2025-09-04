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

package org.apache.geaflow.state.partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;

/**
 * Default implementation of partition manager for label-based partitioning.
 */
public class DefaultPartitionManager implements IPartitionManager {

    private final Map<String, PartitionMetadata> partitionMetadataMap;
    private final Set<String> allPartitions;

    public DefaultPartitionManager() {
        this.partitionMetadataMap = new ConcurrentHashMap<>();
        this.allPartitions = new HashSet<>();
    }

    @Override
    public List<String> parseLabelsFromFilter(IFilter filter) {
        List<String> matchedPartitions = new ArrayList<>();
        
        if (filter == null) {
            // If no filter, return all active partitions
            return new ArrayList<>(getAllPartitions());
        }
        
        if (filter instanceof IGraphFilter) {
            IGraphFilter graphFilter = (IGraphFilter) filter;
            // Extract label information from graph filter
            Set<String> labels = extractLabelsFromGraphFilter(graphFilter);
            
            for (String label : labels) {
                String partitionName = generatePartitionName(label);
                if (partitionExists(partitionName)) {
                    matchedPartitions.add(partitionName);
                }
            }
        }
        
        // If no specific labels found, return all partitions
        if (matchedPartitions.isEmpty()) {
            matchedPartitions.addAll(getAllPartitions());
        }
        
        return matchedPartitions;
    }

    @Override
    public Set<String> getAllPartitions() {
        return new HashSet<>(allPartitions);
    }

    @Override
    public boolean partitionExists(String partitionName) {
        return partitionMetadataMap.containsKey(partitionName);
    }

    @Override
    public PartitionMetadata getPartitionMetadata(String partitionName) {
        return partitionMetadataMap.get(partitionName);
    }

    @Override
    public void registerPartition(String partitionName, PartitionMetadata metadata) {
        partitionMetadataMap.put(partitionName, metadata);
        allPartitions.add(partitionName);
    }

    @Override
    public void unregisterPartition(String partitionName) {
        partitionMetadataMap.remove(partitionName);
        allPartitions.remove(partitionName);
    }

    /**
     * Extract labels from graph filter conditions.
     */
    private Set<String> extractLabelsFromGraphFilter(IGraphFilter graphFilter) {
        Set<String> labels = new HashSet<>();
        
        // This is a simplified implementation
        // In practice, you would need to traverse the filter tree
        // and extract label conditions based on the specific filter implementation
        
        // For now, return empty set to indicate all partitions should be scanned
        return labels;
    }

    /**
     * Generate partition name from label.
     */
    private String generatePartitionName(String label) {
        return "partition_" + label;
    }
}
