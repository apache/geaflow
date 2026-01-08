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

import java.util.List;
import java.util.Set;
import org.apache.geaflow.state.pushdown.filter.IFilter;

/**
 * Interface for managing graph partitions based on labels or other criteria.
 */
public interface IPartitionManager {

    /**
     * Parse labels from filter conditions to determine which partitions to scan.
     * @param filter the filter condition containing label information
     * @return list of partition names that match the filter
     */
    List<String> parseLabelsFromFilter(IFilter filter);

    /**
     * Get all available partition names.
     * @return set of all partition names
     */
    Set<String> getAllPartitions();

    /**
     * Check if a partition exists.
     * @param partitionName name of the partition
     * @return true if partition exists
     */
    boolean partitionExists(String partitionName);

    /**
     * Get partition metadata.
     * @param partitionName name of the partition
     * @return partition metadata or null if not found
     */
    PartitionMetadata getPartitionMetadata(String partitionName);

    /**
     * Register a new partition.
     * @param partitionName name of the partition
     * @param metadata partition metadata
     */
    void registerPartition(String partitionName, PartitionMetadata metadata);

    /**
     * Unregister a partition.
     * @param partitionName name of the partition
     */
    void unregisterPartition(String partitionName);
}
