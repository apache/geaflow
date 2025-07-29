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

import org.apache.geaflow.cluster.protocol.ScheduleStateType;
import org.apache.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;

public abstract class AbstractStateMachine implements IStateMachine<IScheduleState, ICycleSchedulerContext> {

    protected static final ScheduleState START = ScheduleState.of(ScheduleStateType.START);
    protected static final ScheduleState END = ScheduleState.of(ScheduleStateType.END);
    protected StateMachineManager<ScheduleState, ICycleSchedulerContext> stateMachineManager;
    protected ScheduleState currentState;
    protected ICycleSchedulerContext context;


    @Override
    public void init(ICycleSchedulerContext context) {
        this.context = context;
        this.currentState = START;
        ((AbstractCycleSchedulerContext) this.context).setCurrentIterationId(context.getInitialIterationId());
        this.stateMachineManager = new StateMachineManager();
    }

    @Override
    public IScheduleState readyToTransition() {
        if (currentState != END) {
            return transition();
        }
        return END;
    }

    @Override
    public ScheduleState getCurrentState() {
        return currentState;
    }

    /**
     * Transition base on the current context.
     */
    @Override
    public IScheduleState transition() {
        return transition(currentState);
    }

    @Override
    public boolean isTerminated() {
        return currentState == END;
    }

    /**
     * Get a list of state transition path after apply a sequence of transition from the input
     * source.
     */
    private ScheduleState transition(ScheduleState source) {
        ScheduleState target = stateMachineManager.transition(source, context);
        if (target != null) {
            if (END == target) {
                currentState = END;
                return END;
            }

            currentState = ScheduleState.of(target.getScheduleStateType());
            return currentState;
        }
        return null;
    }

    public static class ComputeTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return context.hasNextIteration();
        }
    }
}
