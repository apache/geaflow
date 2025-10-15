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

package org.apache.geaflow.cluster.fetcher;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.PipelineBarrier;

/**
 * Process the barrier event.
 */
public class BarrierHandler implements Serializable {

    private final int taskId;
    private final Map<Integer, ShardInputDesc> inputShards;
    private final Map<Integer, Integer> edgeId2sliceNum;
    private final int inputSliceNum;
    private final Map<Integer, Set<PipelineBarrier>> edgeBarrierCache;
    private final Map<Long, Set<PipelineBarrier>> windowBarrierCache;

    private long finishedWindowId;
    private long totalWindowCount;

    public BarrierHandler(int taskId, Map<Integer, ShardInputDesc> inputShards) {
        this.taskId = taskId;
        this.inputShards = inputShards;
        this.edgeId2sliceNum = inputShards.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSliceNum()));
        this.inputSliceNum = this.edgeId2sliceNum.values().stream().mapToInt(i -> i).sum();
        this.edgeBarrierCache = this.inputShards.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>()));
        this.windowBarrierCache = new HashMap<>();
        this.finishedWindowId = -1;
        this.totalWindowCount = 0;
    }

    public boolean checkCompleted(PipelineBarrier barrier) {
        if (barrier.getWindowId() <= this.finishedWindowId) {
            throw new GeaflowRuntimeException(String.format(
                "illegal state: taskId %s window %s has finished, last finished window is: %s",
                this.taskId, barrier.getWindowId(), this.finishedWindowId));
        }
        int edgeId = barrier.getEdgeId();
        long windowId = barrier.getWindowId();
        Set<PipelineBarrier> edgeBarriers = this.edgeBarrierCache.computeIfAbsent(edgeId, k -> new HashSet<>());
        Set<PipelineBarrier> windowBarriers = this.windowBarrierCache.computeIfAbsent(windowId, k -> new HashSet<>());
        edgeBarriers.add(barrier);
        windowBarriers.add(barrier);

        if (this.inputShards.get(edgeId).isPrefetchWrite()) {
            int barrierSize = edgeBarriers.size();
            if (barrierSize == this.edgeId2sliceNum.get(edgeId)) {
                this.edgeBarrierCache.remove(edgeId);
                edgeBarriers.clear();
                if (this.edgeBarrierCache.isEmpty()) {
                    this.windowBarrierCache.remove(windowId);
                    this.finishedWindowId = windowId;
                    this.totalWindowCount = windowBarriers.stream().mapToLong(PipelineBarrier::getCount).sum();
                    windowBarriers.clear();
                }
                return true;
            }
        } else {
            int barrierSize = windowBarriers.size();
            if (barrierSize == this.inputSliceNum) {
                this.windowBarrierCache.remove(windowId);
                this.finishedWindowId = windowId;
                this.totalWindowCount = windowBarriers.stream().mapToLong(PipelineBarrier::getCount).sum();
                windowBarriers.clear();
                return true;
            }
        }

        return false;
    }

    public long getTotalWindowCount() {
        return totalWindowCount;
    }
}
