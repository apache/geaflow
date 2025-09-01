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
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;

public class FinishIterationEvent extends AbstractExecutableCommand {

    public static final long END_OF_ITERATION_ID = 0;

    public FinishIterationEvent(long schedulerId, int workerId, long windowId, int cycleId) {
        super(schedulerId, workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        worker.init(windowId);
        worker.finish(END_OF_ITERATION_ID);
        ((AbstractWorkerContext) this.context).getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
    }

    @Override
    public EventType getEventType() {
        return EventType.FINISH_ITERATION;
    }

    @Override
    public String toString() {
        return "FinishIterationEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", windowId=" + windowId
            + ", cycleId=" + cycleId
            + '}';
    }
}
