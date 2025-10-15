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

package org.apache.geaflow.runtime.core.scheduler.statemachine;

import java.util.Objects;
import org.apache.geaflow.cluster.protocol.ScheduleStateType;

public class ScheduleState implements IScheduleState {

    private ScheduleStateType stateType;

    public ScheduleState(ScheduleStateType stateType) {
        this.stateType = stateType;
    }

    public static ScheduleState of(ScheduleStateType stateType) {
        return new ScheduleState(stateType);
    }

    @Override
    public ScheduleStateType getScheduleStateType() {
        return stateType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduleState state = (ScheduleState) o;
        return stateType == state.stateType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateType);
    }

    @Override
    public String toString() {
        return stateType.name();
    }
}
