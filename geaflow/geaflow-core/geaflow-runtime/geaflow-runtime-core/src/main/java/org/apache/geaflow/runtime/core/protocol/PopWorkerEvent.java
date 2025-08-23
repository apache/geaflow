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
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.cluster.worker.IAffinityWorker;
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.apache.geaflow.shuffle.IoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pop worker from cache and reuse context.
 */
public class PopWorkerEvent extends AbstractInitCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(PopWorkerEvent.class);

    private final int taskId;

    public PopWorkerEvent(long schedulerId,
                          int workerId,
                          int cycleId,
                          long windowId,
                          long pipelineId,
                          String pipelineName,
                          IoDescriptor ioDescriptor,
                          int taskId) {
        super(schedulerId, workerId, cycleId, windowId, pipelineId, pipelineName, ioDescriptor);
        this.taskId = taskId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        LOGGER.info("reuse worker context, taskId {}", taskId);
        AbstractWorkerContext popWorkerContext = new WorkerContext(taskContext);
        popWorkerContext.setPipelineId(pipelineId);
        popWorkerContext.setPipelineName(pipelineName);
        popWorkerContext.setWindowId(windowId);
        popWorkerContext.setTaskId(taskId);
        popWorkerContext.setSchedulerId(schedulerId);

        ((IAffinityWorker) worker).pop(popWorkerContext);
        context = worker.getWorkerContext();

        this.initFetcher();
        this.popEmitter();
    }

    @Override
    public EventType getEventType() {
        return EventType.POP_WORKER;
    }

    public long getWindowId() {
        return windowId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    @Override
    public String toString() {
        return "PopWorkerEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + '}';
    }
}
