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

/**
 * Send from scheduler to cycle head task to launch the first iteration of the cycle.
 */
public class ExecuteFirstIterationEvent extends AbstractExecutableCommand {

    public ExecuteFirstIterationEvent(long schedulerId, int workerId, int cycleId, long windowId) {
        super(schedulerId, workerId, cycleId, windowId);
    }

    @Override
    public EventType getEventType() {
        return EventType.EXECUTE_FIRST_ITERATION;
    }

    @Override
    public String toString() {
        return "ExecuteFirstIterationEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }
}
