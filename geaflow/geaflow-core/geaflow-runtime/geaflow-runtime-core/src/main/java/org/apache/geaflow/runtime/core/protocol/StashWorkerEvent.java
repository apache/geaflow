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

import org.apache.geaflow.cluster.collector.StashEmitterRequest;
import org.apache.geaflow.cluster.fetcher.CloseFetchRequest;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.cluster.worker.IAffinityWorker;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stash worker.
 */
public class StashWorkerEvent extends AbstractCleanCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(StashWorkerEvent.class);

    private final int taskId;

    public StashWorkerEvent(long schedulerId, int workerId, int cycleId, long windowId, int taskId) {
        super(schedulerId, workerId, cycleId, windowId);
        this.taskId = taskId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        ExecutionTask executionTask = workerContext.getExecutionTask();
        workerContext.getEventMetrics().setFinishTime(System.currentTimeMillis());
        LOGGER.info("stash task {} {}/{} of {} {} : {}",
            executionTask.getTaskId(),
            executionTask.getIndex(),
            executionTask.getParallelism(),
            executionTask.getVertexId(),
            executionTask.getProcessor().toString(),
            workerContext.getEventMetrics());

        // Stash worker context.
        ((IAffinityWorker) worker).stash();

        this.fetcherRunner.add(new CloseFetchRequest(this.taskId));
        this.emitterRunner.add(new StashEmitterRequest(this.taskId, this.windowId));
        worker.close();
        LOGGER.info("stash worker context, taskId {}", ((WorkerContext) context).getTaskId());

        this.sendDoneEvent(workerContext.getDriverId(), EventType.CLEAN_CYCLE, null, true);
    }

    @Override
    public EventType getEventType() {
        return EventType.STASH_WORKER;
    }

    @Override
    public String toString() {
        return "StashWorkerEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
