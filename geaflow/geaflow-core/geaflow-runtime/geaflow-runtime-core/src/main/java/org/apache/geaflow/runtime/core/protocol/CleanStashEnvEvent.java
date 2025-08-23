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
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.shuffle.service.ShuffleManager;

public class CleanStashEnvEvent extends AbstractCleanCommand {

    protected final long pipelineId;
    protected final String driverId;

    public CleanStashEnvEvent(long schedulerId, int workerId, int cycleId, long iterationId,
                              long pipelineId, String driverId) {
        super(schedulerId, workerId, cycleId, iterationId);
        this.pipelineId = pipelineId;
        this.driverId = driverId;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        ShuffleManager.getInstance().release(pipelineId);
        this.sendDoneEvent(this.driverId, EventType.CLEAN_ENV, null, false);
    }

    @Override
    public EventType getEventType() {
        return EventType.CLEAN_ENV;
    }

    public void setIterationId(int iterationId) {
        this.windowId = iterationId;
    }

    @Override
    protected <T> void sendDoneEvent(String driverId, EventType sourceEventType, T result, boolean sendMetrics) {
        DoneEvent<T> doneEvent = new DoneEvent<>(this.schedulerId, this.cycleId, this.windowId, 0, sourceEventType, result);
        RpcClient.getInstance().processPipeline(driverId, doneEvent);
    }

    @Override
    public String toString() {
        return "CleanStashEnvEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", pipelineId=" + pipelineId
            + '}';
    }
}
