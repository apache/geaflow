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

package org.apache.geaflow.runtime.core.scheduler.statemachine.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.protocol.ScheduleStateType;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.runtime.core.scheduler.ExecutableEventIterator.ExecutableEvent;
import org.apache.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.CheckpointSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.statemachine.AbstractStateMachine;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ComposeState;
import org.apache.geaflow.runtime.core.scheduler.statemachine.IScheduleState;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ITransitionCondition;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ScheduleState;

/**
 * Holds all state and transitions of the schedule state machine.
 */
public class PipelineStateMachine extends AbstractStateMachine {

    private static final ScheduleState INIT = ScheduleState.of(ScheduleStateType.INIT);
    private static final ScheduleState PREFETCH = ScheduleState.of(ScheduleStateType.PREFETCH);
    private static final ScheduleState FINISH_PREFETCH =
        ScheduleState.of(ScheduleStateType.FINISH_PREFETCH);
    private static final ScheduleState ITERATION_INIT = ScheduleState.of(ScheduleStateType.ITERATION_INIT);
    private static final ScheduleState EXECUTE_COMPUTE = ScheduleState.of(ScheduleStateType.EXECUTE_COMPUTE);
    private static final ScheduleState ROLLBACK = ScheduleState.of(ScheduleStateType.ROLLBACK);
    private static final ScheduleState ITERATION_FINISH = ScheduleState.of(ScheduleStateType.ITERATION_FINISH);
    private static final ScheduleState CLEAN_CYCLE = ScheduleState.of(ScheduleStateType.CLEAN_CYCLE);

    @Override
    public void init(ICycleSchedulerContext context) {
        super.init(context);

        // Build state machine.
        // START -> ROLLBACK | PREFETCH | INIT.
        this.stateMachineManager.addTransition(START, ROLLBACK, new Start2RollbackTransitionCondition());
        this.stateMachineManager.addTransition(START, PREFETCH, new Start2PrefetchTransitionCondition());
        this.stateMachineManager.addTransition(START, INIT);

        // PREFETCH -> ITERATION_FINISH | INIT.
        this.stateMachineManager.addTransition(PREFETCH, ITERATION_FINISH, new FinishTransitionCondition());
        this.stateMachineManager.addTransition(PREFETCH, INIT);

        // INIT -> ROLLBACK | ITERATION_INIT | EXECUTE_COMPUTE.
        this.stateMachineManager.addTransition(INIT, ROLLBACK, new Init2RollbackTransitionCondition());
        this.stateMachineManager.addTransition(INIT, ITERATION_INIT, new InitIterationTransitionCondition());
        this.stateMachineManager.addTransition(INIT, EXECUTE_COMPUTE);

        // ROLLBACK -> ITERATION_INIT | EXECUTE_COMPUTE.
        this.stateMachineManager.addTransition(ROLLBACK, ITERATION_INIT, new InitIterationTransitionCondition());
        this.stateMachineManager.addTransition(ROLLBACK, EXECUTE_COMPUTE);

        // ITERATION_INIT -> EXECUTE_COMPUTE ｜ PREFETCH | ITERATION_FINISH.
        this.stateMachineManager.addTransition(ITERATION_INIT, EXECUTE_COMPUTE, new ComputeTransitionCondition());
        this.stateMachineManager.addTransition(ITERATION_INIT, PREFETCH, new Compute2PrefetchTransitionCondition());
        this.stateMachineManager.addTransition(ITERATION_INIT, ITERATION_FINISH, new FinishTransitionCondition());

        // EXECUTE_COMPUTE -> EXECUTE_COMPUTE | ITERATION_FINISH | FINISH_PREFETCH | CLEAN_CYCLE.
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, EXECUTE_COMPUTE, new ComputeTransitionCondition());
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, PREFETCH, new Compute2PrefetchTransitionCondition());
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, ITERATION_FINISH, new FinishTransitionCondition());
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, FINISH_PREFETCH,
            new Compute2FinishPrefetchTransitionCondition());
        this.stateMachineManager.addTransition(EXECUTE_COMPUTE, CLEAN_CYCLE, new CleanTransitionCondition());

        // ITERATION_FINISH -> FINISH_PREFETCH | CLEAN_CYCLE.
        this.stateMachineManager.addTransition(ITERATION_FINISH, FINISH_PREFETCH,
            new FinishPrefetchTransitionCondition());
        this.stateMachineManager.addTransition(ITERATION_FINISH, CLEAN_CYCLE);

        // FINISH_PREFETCH -> CLEAN_CYCLE.
        this.stateMachineManager.addTransition(FINISH_PREFETCH, CLEAN_CYCLE);

        // CLEAN_CYCLE -> END.
        this.stateMachineManager.addTransition(CLEAN_CYCLE, END);
    }

    @Override
    public IScheduleState transition() {
        List<IScheduleState> states = new ArrayList<>();
        transition(currentState, states);
        if (states.isEmpty()) {
            return null;
        } else {
            if (states.size() == 1) {
                return states.get(0);
            } else {
                return ComposeState.of(states);
            }
        }
    }

    private void transition(ScheduleState source, List<IScheduleState> results) {
        ScheduleState target = stateMachineManager.transition(source, context);
        if (target != null) {
            if (END == target) {
                currentState = END;
                return;
            }

            // Not allow two execution state compose.
            if (!composable(results.isEmpty() ? null : (ScheduleState) results.get(results.size() - 1), target)) {
                return;
            }

            currentState = ScheduleState.of(target.getScheduleStateType());
            results.add(currentState);
            if (target.getScheduleStateType() == ScheduleStateType.ITERATION_FINISH && source.getScheduleStateType() == ScheduleStateType.PREFETCH) {
                return;
            }
            transition(currentState, results);
        }
    }

    private boolean composable(ScheduleState previous, ScheduleState current) {
        if (previous == null || current == null) {
            return true;
        }
        /*if (context.getCycle() instanceof ExecutionNodeCycle) {
            if (((ExecutionNodeCycle) context.getCycle()).getVertexGroup().getVertexMap().size() > 1) {
                return false;
            }
        }*/
        // Not allow two execution state compose.
        if ((previous.getScheduleStateType() == ScheduleStateType.ITERATION_INIT || previous.getScheduleStateType() == ScheduleStateType.EXECUTE_COMPUTE)
            && current.getScheduleStateType() == ScheduleStateType.EXECUTE_COMPUTE) {
            return false;
        }
        return true;
    }

    public static class Start2RollbackTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            if (context instanceof CheckpointSchedulerContext) {
                return context.isRecovered();
            }
            return false;
        }
    }

    public static class Start2PrefetchTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return context.isPrefetch() && !((ExecutionNodeCycle) context.getCycle()).isIterative();
        }
    }

    public static class Compute2PrefetchTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            // When the iteration is finished and prefetch is enable, we need to prefetch for next iteration.
            return context.isCycleFinished() && (context.getCycle().getType() == ExecutionCycleType.ITERATION
                || context.getCycle().getType() == ExecutionCycleType.ITERATION_WITH_AGG) && context.isPrefetch();
        }
    }

    public static class Init2RollbackTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            if (context.isRollback()) {
                ((AbstractCycleSchedulerContext) context).setRollback(false);
                return true;
            }
            return false;
        }
    }

    public static class InitIterationTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return ((ExecutionNodeCycle) context.getCycle()).getVertexGroup().getCycleGroupMeta().isIterative();
        }
    }

    public static class FinishTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return context.isCycleFinished() && (context.getCycle().getType() == ExecutionCycleType.ITERATION
                || context.getCycle().getType() == ExecutionCycleType.ITERATION_WITH_AGG);
        }
    }

    public static class FinishPrefetchTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            if (context.getConfig().getBoolean(ExecutionConfigKeys.SHUFFLE_PREFETCH)) {
                Map<Integer, ExecutableEvent> needFinishedPrefetchEvents = context.getPrefetchEvents();
                return needFinishedPrefetchEvents != null && needFinishedPrefetchEvents.size() > 0;
            }
            return false;
        }
    }

    public static class CleanTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            return context.isCycleFinished() && !(context.getCycle().getType() == ExecutionCycleType.ITERATION
                || context.getCycle().getType() == ExecutionCycleType.ITERATION_WITH_AGG);
        }
    }

    public static class Compute2FinishPrefetchTransitionCondition
        implements ITransitionCondition<ScheduleState, ICycleSchedulerContext> {

        @Override
        public boolean predicate(ScheduleState state, ICycleSchedulerContext context) {
            if (context.isCycleFinished() && !(context.getCycle().getType() == ExecutionCycleType.ITERATION
                || context.getCycle().getType() == ExecutionCycleType.ITERATION_WITH_AGG) && context.isPrefetch()) {
                Map<Integer, ExecutableEvent> needFinishedPrefetchEvents = context.getPrefetchEvents();
                return needFinishedPrefetchEvents != null && needFinishedPrefetchEvents.size() > 0;
            }
            return false;
        }
    }

}
