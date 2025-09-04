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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;

/**
 * Interface for iterating over data from multiple partitions.
 * This interface extends CloseableIterator to provide partition-aware functionality.
 *
 * @param <T> the type of elements being iterated
 */
public interface IMultiPartitionIterator<T> extends CloseableIterator<T>, Serializable {

    /**
     * Get the list of active partition names.
     *
     * @return list of active partition names
     */
    List<String> getActivePartitions();

    /**
     * Add a partition with its iterator.
     *
     * @param partitionName the name of the partition
     * @param iterator the iterator for the partition
     */
    void addPartition(String partitionName, CloseableIterator<T> iterator);

    /**
     * Remove a partition.
     *
     * @param partitionName the name of the partition to remove
     */
    void removePartition(String partitionName);

    /**
     * Get statistics for all partitions.
     *
     * @return map of partition name to record count
     */
    Map<String, Long> getPartitionStats();

    /**
     * Check if a partition is active.
     *
     * @param partitionName the name of the partition
     * @return true if the partition is active, false otherwise
     */
    boolean isPartitionActive(String partitionName);

    /**
     * Get the total number of partitions.
     *
     * @return the number of partitions
     */
    int getPartitionCount();
}
