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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.common.shuffle.BatchPhase;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.ha.service.HAServiceFactory;
import org.apache.geaflow.ha.service.ResourceData;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.ISliceMeta;
import org.apache.geaflow.shuffle.message.LogicalPipelineSliceMeta;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.message.SliceId;

public class InitFetchRequest implements IFetchRequest {

    private final long pipelineId;
    private final String pipelineName;
    private final int vertexId;
    private final int taskId;
    private final int taskIndex;
    private final int taskParallelism;
    private final String taskName;
    private final Map<Integer, ShardInputDesc> inputShards;
    private final Map<Integer, BatchPhase> shufflePhases;
    private final int totalSliceNum;
    private final Map<Integer, List<PipelineSliceMeta>> inputSlices;
    private final List<IInputMessageBuffer<?>> fetchListeners;

    public InitFetchRequest(long pipelineId,
                            String pipelineName,
                            int vertexId,
                            int taskId,
                            int taskIndex,
                            int taskParallelism,
                            String taskName,
                            Map<Integer, ShardInputDesc> inputShards) {
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.vertexId = vertexId;
        this.taskId = taskId;
        this.taskIndex = taskIndex;
        this.taskParallelism = taskParallelism;
        this.taskName = taskName;
        this.inputShards = inputShards;
        this.shufflePhases = this.inputShards.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBatchPhase()));
        Tuple<Integer, Map<Integer, List<PipelineSliceMeta>>> tuple = buildSlices(this.inputShards);
        this.totalSliceNum = tuple.f0;
        this.inputSlices = tuple.f1;
        this.fetchListeners = new ArrayList<>();
    }

    @Override
    public int getTaskId() {
        return this.taskId;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.INIT;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }

    public String getPipelineName() {
        return this.pipelineName;
    }

    public int getVertexId() {
        return this.vertexId;
    }

    public int getTaskIndex() {
        return this.taskIndex;
    }

    public int getTaskParallelism() {
        return this.taskParallelism;
    }

    public String getTaskName() {
        return this.taskName;
    }

    public Map<Integer, ShardInputDesc> getInputShards() {
        return this.inputShards;
    }

    public Map<Integer, BatchPhase> getShufflePhases() {
        return this.shufflePhases;
    }

    public int getSliceNum() {
        return this.totalSliceNum;
    }

    public Map<Integer, List<PipelineSliceMeta>> getInputSlices() {
        return this.inputSlices;
    }

    public List<IInputMessageBuffer<?>> getFetchListeners() {
        return this.fetchListeners;
    }

    public void addListener(IInputMessageBuffer<?> listener) {
        this.fetchListeners.add(listener);
    }

    private static Tuple<Integer, Map<Integer, List<PipelineSliceMeta>>> buildSlices(Map<Integer, ShardInputDesc> inputShards) {
        int totalSliceNum = 0;
        Map<Integer, List<PipelineSliceMeta>> inputSlices = new HashMap<>();

        for (Map.Entry<Integer, ShardInputDesc> entry : inputShards.entrySet()) {
            Integer edgeId = entry.getKey();
            ShardInputDesc inputDesc = entry.getValue();
            List<Shard> shards = inputDesc.getInput();
            List<PipelineSliceMeta> slices = new ArrayList<>();
            for (Shard shard : shards) {
                for (ISliceMeta slice : shard.getSlices()) {
                    if (slice instanceof LogicalPipelineSliceMeta) {
                        // Convert to physical shuffle slice meta.
                        LogicalPipelineSliceMeta logicalPipelineSliceMeta = (LogicalPipelineSliceMeta) slice;
                        slices.add(toPhysicalSliceMeta(logicalPipelineSliceMeta));
                    } else {
                        slices.add((PipelineSliceMeta) slice);
                    }
                }
            }
            inputSlices.put(edgeId, slices);
            totalSliceNum += inputDesc.getSliceNum();
        }

        return Tuple.of(totalSliceNum, inputSlices);
    }

    private static PipelineSliceMeta toPhysicalSliceMeta(LogicalPipelineSliceMeta sliceMeta) {
        String containerId = sliceMeta.getContainerId();
        SliceId sliceId = sliceMeta.getSliceId();
        long windowId = sliceMeta.getWindowId();
        ResourceData resourceData = HAServiceFactory.getService().resolveResource(containerId);
        return new PipelineSliceMeta(sliceId, windowId,
            new ShuffleAddress(resourceData.getHost(), resourceData.getShufflePort()));
    }

}
