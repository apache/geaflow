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

package org.apache.geaflow.state.iterator;

import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;

/**
 * Multi-partition iterator interface for handling data across multiple partitions.
 */
public interface IMultiPartitionIterator<T> extends CloseableIterator<T> {

    /**
     * Get list of active partition names.
     * @return list of partition names currently being scanned
     */
    List<String> getActivePartitions();

    /**
     * Add a partition iterator to the multi-partition iterator.
     * @param partitionName name of the partition
     * @param iterator iterator for the specific partition
     */
    void addPartition(String partitionName, CloseableIterator<T> iterator);

    /**
     * Remove a partition from scanning.
     * @param partitionName name of the partition to remove
     */
    void removePartition(String partitionName);

    /**
     * Get statistics for each partition.
     * @return map of partition name to record count
     */
    Map<String, Long> getPartitionStats();

    /**
     * Check if a specific partition is active.
     * @param partitionName partition name to check
     * @return true if partition is active
     */
    boolean isPartitionActive(String partitionName);

    /**
     * Get total number of partitions.
     * @return total partition count
     */
    int getPartitionCount();
}
