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

import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEventContext;
import org.apache.geaflow.cluster.protocol.IHighAvailableEvent;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.runtime.core.context.EventContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.apache.geaflow.shuffle.IoDescriptor;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitCycleEvent extends AbstractInitCommand implements IHighAvailableEvent {

    private final ExecutionTask task;
    private final String driverId;
    private final HighAvailableLevel haLevel;

    public InitCycleEvent(long schedulerId,
                          int workerId,
                          int cycleId,
                          long iterationId,
                          long pipelineId,
                          String pipelineName,
                          IoDescriptor ioDescriptor,
                          ExecutionTask task,
                          String driverId,
                          HighAvailableLevel haLevel) {
        super(schedulerId, workerId, cycleId, iterationId, pipelineId, pipelineName, ioDescriptor);
        this.task = task;
        this.driverId = driverId;
        this.haLevel = haLevel;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        this.task.buildTaskName(this.pipelineName, this.cycleId, this.windowId);
        this.context = this.initContext(taskContext);
        this.initFetcher();
        this.initEmitter();
        this.worker.open(this.context);
    }

    private WorkerContext initContext(ITaskContext taskContext) {
        WorkerContext workerContext = new WorkerContext(taskContext);
        IEventContext eventContext = EventContext.builder()
            .withExecutionTask(this.task)
            .withDriverId(this.driverId)
            .withCycleId(this.cycleId)
            .withIoDescriptor(this.ioDescriptor)
            .withPipelineId(this.pipelineId)
            .withCurrentWindowId(this.windowId)
            .withPipelineName(this.pipelineName)
            .withWindowId(this.windowId)
            .withSchedulerId(this.schedulerId)
            .build();
        workerContext.init(eventContext);
        return workerContext;
    }

    @Override
    public EventType getEventType() {
        return EventType.INIT_CYCLE;
    }

    @Override
    public HighAvailableLevel getHaLevel() {
        return haLevel;
    }

    public ExecutionTask getTask() {
        return task;
    }

    @Override
    public String toString() {
        return "InitCycleEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + '}';
    }

}
