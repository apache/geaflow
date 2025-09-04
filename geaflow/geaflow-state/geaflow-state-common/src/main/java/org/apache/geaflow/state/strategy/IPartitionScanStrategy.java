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

import org.apache.geaflow.state.pushdown.filter.IFilter;

import java.util.List;

/**
 * Interface for optimizing partition scanning strategies.
 * This interface provides methods for determining scan order, parallel scanning decisions,
 * and cost estimation for partition-based operations.
 */
public interface IPartitionScanStrategy {

    /**
     * Optimize the scan order of partitions based on the filter and partition characteristics.
     *
     * @param partitionNames list of partition names to scan
     * @param filter the filter to apply
     * @return optimized list of partition names in scan order
     */
    List<String> optimizeScanOrder(List<String> partitionNames, IFilter filter);

    /**
     * Determine whether parallel scanning should be used for the given partitions.
     *
     * @param partitionNames list of partition names to scan
     * @return true if parallel scanning should be used, false otherwise
     */
    boolean shouldUseParallelScanning(List<String> partitionNames);

    /**
     * Get the maximum number of partitions that can be scanned in parallel.
     *
     * @return maximum number of parallel partitions
     */
    int getMaxParallelPartitions();

    /**
     * Estimate the cost of scanning the specified partitions.
     *
     * @param partitionNames list of partition names to scan
     * @return estimated scan cost (e.g., number of records, time estimate)
     */
    long estimateScanCost(List<String> partitionNames);
}
