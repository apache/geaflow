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
import org.apache.geaflow.shuffle.IoDescriptor;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitIterationEvent extends AbstractInitCommand {

    public InitIterationEvent(long schedulerId,
                              int workerId,
                              int cycleId,
                              long iterationId,
                              long pipelineId,
                              String pipelineName,
                              IoDescriptor ioDescriptor) {
        super(schedulerId, workerId, cycleId, iterationId, pipelineId, pipelineName, ioDescriptor);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        this.initFetcher();
    }

    @Override
    public EventType getEventType() {
        return EventType.INIT_ITERATION;
    }

    @Override
    public String toString() {
        return "InitIterationEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", iterationId=" + windowId
            + '}';
    }
}
