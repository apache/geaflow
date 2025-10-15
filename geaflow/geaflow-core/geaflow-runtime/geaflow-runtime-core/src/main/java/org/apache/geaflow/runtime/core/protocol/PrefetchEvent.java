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

package org.apache.geaflow.runtime.core.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.fetcher.FetchRequest;
import org.apache.geaflow.cluster.fetcher.InitFetchRequest;
import org.apache.geaflow.cluster.fetcher.PrefetchMessageBuffer;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.runtime.core.worker.PrefetchCallbackHandler;
import org.apache.geaflow.shuffle.InputDescriptor;
import org.apache.geaflow.shuffle.IoDescriptor;
import org.apache.geaflow.shuffle.desc.IInputDesc;
import org.apache.geaflow.shuffle.desc.InputType;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.SliceId;

public class PrefetchEvent extends AbstractInitCommand {

    private final ExecutionTask task;
    private final List<Integer> edgeIds;

    public PrefetchEvent(long schedulerId,
                         int workerId,
                         int cycleId,
                         long windowId,
                         long pipelineId,
                         String pipelineName,
                         ExecutionTask task,
                         IoDescriptor ioDescriptor) {
        super(schedulerId, workerId, cycleId, windowId, pipelineId, pipelineName, ioDescriptor);
        this.task = task;
        this.edgeIds = new ArrayList<>(ioDescriptor.getInputDescriptor().getInputDescMap().keySet());
    }

    public List<Integer> getEdgeIds() {
        return this.edgeIds;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        this.task.buildTaskName(this.pipelineName, this.cycleId, this.windowId);
        InitFetchRequest initFetchRequest = this.buildInitFetchRequest(
            this.ioDescriptor.getInputDescriptor(), this.task, null);
        this.fetcherRunner.add(initFetchRequest);
        this.fetcherRunner.add(new FetchRequest(this.task.getTaskId(), this.windowId, 1));
    }

    @Override
    protected InitFetchRequest buildInitFetchRequest(InputDescriptor inputDescriptor, ExecutionTask task, EventMetrics eventMetrics) {
        Map<Integer, ShardInputDesc> inputDescMap = new HashMap<>();
        for (Map.Entry<Integer, IInputDesc<?>> entry : inputDescriptor.getInputDescMap().entrySet()) {
            IInputDesc<?> inputDesc = entry.getValue();
            if (inputDesc.getInputType() == InputType.META) {
                inputDescMap.put(entry.getKey(), (ShardInputDesc) entry.getValue());
            }
        }

        InitFetchRequest initFetchRequest = new InitFetchRequest(
            this.pipelineId,
            this.pipelineName,
            task.getVertexId(),
            task.getTaskId(),
            task.getIndex(),
            task.getParallelism(),
            task.getTaskName(),
            inputDescMap);

        for (Map.Entry<Integer, ShardInputDesc> entry : inputDescMap.entrySet()) {
            Integer edgeId = entry.getKey();
            SliceId sliceId = new SliceId(this.pipelineId, edgeId, -1, task.getIndex());
            PrefetchMessageBuffer<Object> prefetchMessageBuffer = new PrefetchMessageBuffer<>(task.getTaskName(), sliceId);
            initFetchRequest.addListener(prefetchMessageBuffer);
            PrefetchCallbackHandler.getInstance()
                .registerTaskEventCallback(sliceId, new PrefetchCallbackHandler.PrefetchCallback(prefetchMessageBuffer));
        }

        return initFetchRequest;
    }

    @Override
    public EventType getEventType() {
        return EventType.PREFETCH;
    }

    @Override
    public String toString() {
        return "PrefetchEvent{"
            + "taskId=" + this.task.getTaskId()
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + '}';
    }

}
