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
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send from scheduler to cycle head task to launch one iteration of the cycle.
 */
public class LaunchSourceEvent extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(LaunchSourceEvent.class);

    public LaunchSourceEvent(long schedulerId, int workerId, int cycleId, long windowId) {
        super(schedulerId, workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        worker.init(windowId);
        if (!workerContext.isFinished()) {
            boolean hasData = (boolean) worker.process(null);
            if (!hasData) {
                workerContext.setFinished(true);
                this.sendDoneEvent(
                    workerContext.getDriverId(),
                    EventType.LAUNCH_SOURCE,
                    false,
                    false);
                LOGGER.info("source is finished at {}, workerId {}, taskId {}",
                    windowId, workerId, workerContext.getTaskId());
            }
        } else {
            this.sendDoneEvent(
                workerContext.getDriverId(),
                EventType.LAUNCH_SOURCE,
                false,
                false);
            LOGGER.info("source already finished, workerId {}, taskId {}",
                workerId,
                workerContext.getTaskId());
        }
        worker.finish(windowId);
        workerContext.getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
    }

    @Override
    public EventType getEventType() {
        return EventType.LAUNCH_SOURCE;
    }

    @Override
    public String toString() {
        return "LaunchSourceEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
