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

import org.apache.geaflow.cluster.fetcher.CloseFetchRequest;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;

public class LoadGraphProcessEvent extends AbstractIterationComputeCommand {

    public LoadGraphProcessEvent(long schedulerId, int workerId, int cycleId, long windowId, long fetchWindowId, long fetchCount) {
        super(schedulerId, workerId, cycleId, windowId, fetchWindowId, fetchCount);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        this.fetcherRunner.add(new CloseFetchRequest(((WorkerContext) this.context).getTaskId()));
    }

    @Override
    public EventType getEventType() {
        return EventType.PRE_GRAPH_PROCESS;
    }

    @Override
    public String toString() {
        return "LoadGraphProcessEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", fetchWindowId=" + fetchWindowId
            + '}';
    }
}
