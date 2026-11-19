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
import org.apache.geaflow.state.pushdown.filter.IFilter;

/**
 * Strategy interface for partition scanning optimization.
 */
public interface IPartitionScanStrategy {

    /**
     * Determine the optimal scanning order for partitions.
     * @param partitionNames list of partition names to scan
     * @param filter filter conditions that may affect scanning order
     * @return optimized list of partition names in scanning order
     */
    List<String> optimizeScanOrder(List<String> partitionNames, IFilter filter);

    /**
     * Check if parallel scanning is beneficial for the given partitions.
     * @param partitionNames list of partition names
     * @return true if parallel scanning should be used
     */
    boolean shouldUseParallelScanning(List<String> partitionNames);

    /**
     * Get the maximum number of partitions to scan in parallel.
     * @return maximum parallel partition count
     */
    int getMaxParallelPartitions();

    /**
     * Estimate the cost of scanning the given partitions.
     * @param partitionNames list of partition names
     * @return estimated cost (lower is better)
     */
    long estimateScanCost(List<String> partitionNames);
}
