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

package org.apache.geaflow.runtime.core.scheduler.statemachine.graph;

import org.apache.geaflow.cluster.protocol.ScheduleStateType;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.statemachine.AbstractStateMachine;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ITransitionCondition;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ScheduleState;

/**
 * Holds all state and transitions of the schedule state machine.
 */
public class GraphStateMachine extends AbstractStateMachine {

    private static final ScheduleState EXECUTE_COMPUTE = ScheduleState.of(ScheduleStateType.EXECUTE_COMPUTE);

    @Override
    public void init(ICycleSchedulerContext context) {
        super.init(context);
        // Build state machine.
        // START -> EXECUTE_COMPUTE.
        this.stateMachineManager.addTransition(START, EXECUTE_COMPUTE, new ComputeTransitionCondition());

        // EXECUTE_COMPUTE -> CLEAN_PIPELINE | CLEAN_PIPELINE.
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, EXECUTE_COMPUTE, new ComputeTransitionCondition());
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, END, new FinishTransitionCondition());
    }

    public static class FinishTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return context.isCycleFinished();
        }
    }
}
