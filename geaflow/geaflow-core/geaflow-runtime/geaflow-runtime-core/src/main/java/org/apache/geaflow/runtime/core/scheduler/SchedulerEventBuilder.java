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

package org.apache.geaflow.runtime.core.scheduler;

import static org.apache.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext.DEFAULT_INITIAL_ITERATION_ID;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.protocol.ScheduleStateType;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.util.ExecutionTaskUtils;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.runtime.core.protocol.CleanCycleEvent;
import org.apache.geaflow.runtime.core.protocol.ComposeEvent;
import org.apache.geaflow.runtime.core.protocol.ExecuteComputeEvent;
import org.apache.geaflow.runtime.core.protocol.ExecuteFirstIterationEvent;
import org.apache.geaflow.runtime.core.protocol.FinishIterationEvent;
import org.apache.geaflow.runtime.core.protocol.FinishPrefetchEvent;
import org.apache.geaflow.runtime.core.protocol.InitCollectCycleEvent;
import org.apache.geaflow.runtime.core.protocol.InitCycleEvent;
import org.apache.geaflow.runtime.core.protocol.InitIterationEvent;
import org.apache.geaflow.runtime.core.protocol.InterruptTaskEvent;
import org.apache.geaflow.runtime.core.protocol.IterationExecutionComputeWithAggEvent;
import org.apache.geaflow.runtime.core.protocol.LaunchSourceEvent;
import org.apache.geaflow.runtime.core.protocol.LoadGraphProcessEvent;
import org.apache.geaflow.runtime.core.protocol.PopWorkerEvent;
import org.apache.geaflow.runtime.core.protocol.PrefetchEvent;
import org.apache.geaflow.runtime.core.protocol.RollbackCycleEvent;
import org.apache.geaflow.runtime.core.protocol.StashWorkerEvent;
import org.apache.geaflow.runtime.core.scheduler.ExecutableEventIterator.ExecutableEvent;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.cycle.CollectExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.io.CycleResultManager;
import org.apache.geaflow.runtime.core.scheduler.io.IoDescriptorBuilder;
import org.apache.geaflow.shuffle.IoDescriptor;
import org.apache.geaflow.shuffle.desc.OutputType;

public class SchedulerEventBuilder {

    private static final int COMPUTE_FETCH_COUNT = 1;

    private final ICycleSchedulerContext<ExecutionNodeCycle, ExecutionGraphCycle, ?> context;
    private final ExecutionNodeCycle cycle;
    private final CycleResultManager resultManager;
    private final boolean enableAffinity;
    private final boolean isIteration;
    private final long schedulerId;

    public SchedulerEventBuilder(ICycleSchedulerContext<ExecutionNodeCycle, ExecutionGraphCycle, ?> context,
                                 CycleResultManager resultManager,
                                 long schedulerId) {
        this.context = context;
        this.cycle = context.getCycle();
        this.resultManager = resultManager;
        this.enableAffinity = context.getParentContext() != null
            && context.getParentContext().getCycle().getIterationCount() > 1;
        this.isIteration = cycle.getVertexGroup().getCycleGroupMeta().isIterative();
        this.schedulerId = schedulerId;
    }

    public ExecutableEventIterator build(ScheduleStateType state, long iterationId) {
        switch (state) {
            case PREFETCH:
                return this.buildPrefetch();
            case INIT:
                return this.buildInitPipeline();
            case ITERATION_INIT:
                return buildInitIteration(iterationId);
            case EXECUTE_COMPUTE:
                return buildExecute(iterationId);
            case ITERATION_FINISH:
                return this.finishIteration();
            case FINISH_PREFETCH:
                return this.buildFinishPrefetch();
            case CLEAN_CYCLE:
                return this.finishPipeline();
            case ROLLBACK:
                return this.handleRollback();
            default:
                throw new GeaflowRuntimeException(String.format("not support event %s yet", state));
        }

    }

    private ExecutableEventIterator buildPrefetch() {
        ExecutableEventIterator iterator = this.buildChildrenPrefetchEvent();
        return iterator;
    }

    private ExecutableEventIterator buildFinishPrefetch() {
        ExecutableEventIterator events = new ExecutableEventIterator();
        Map<Integer, ExecutableEvent> needFinishedPrefetchEvents =
            this.context.getPrefetchEvents();
        Iterator<Entry<Integer, ExecutableEvent>> iterator = needFinishedPrefetchEvents.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, ExecutableEvent> entry = iterator.next();
            ExecutableEvent executableEvent = entry.getValue();
            IEvent event = executableEvent.getEvent();
            PrefetchEvent prefetchEvent = (PrefetchEvent) event;
            FinishPrefetchEvent finishPrefetchEvent = new FinishPrefetchEvent(
                prefetchEvent.getSchedulerId(),
                prefetchEvent.getWorkerId(),
                prefetchEvent.getCycleId(),
                prefetchEvent.getIterationWindowId(),
                executableEvent.getTask().getTaskId(),
                executableEvent.getTask().getIndex(),
                prefetchEvent.getPipelineId(),
                prefetchEvent.getEdgeIds());
            ExecutableEvent finishExecutableEvent = ExecutableEvent.build(
                executableEvent.getWorker(), executableEvent.getTask(), finishPrefetchEvent);
            events.addEvent(finishExecutableEvent);
            iterator.remove();
        }
        return events;
    }

    private ExecutableEventIterator buildInitPipeline() {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (ExecutionTask task : this.cycle.getTasks()) {
            IoDescriptor ioDescriptor =
                IoDescriptorBuilder.buildPipelineIoDescriptor(task, this.cycle,
                    this.resultManager, this.context.isPrefetch());
            iterator.addEvent(task.getWorkerInfo(), task, buildInitOrPopEvent(task, ioDescriptor));
        }
        return iterator;
    }

    private ExecutableEventIterator buildChildrenPrefetchEvent() {
        ICycleSchedulerContext<ExecutionGraphCycle, ?, ?> parentContext = this.context.getParentContext();
        Set<Integer> childrenIds = new HashSet<>(parentContext.getCycle().getCycleChildren().get(this.cycle.getCycleId()));
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (Integer childId : childrenIds) {
            IExecutionCycle childCycle = parentContext.getCycle().getCycleMap().get(childId);
            if (childCycle instanceof ExecutionNodeCycle) {
                ExecutionNodeCycle childNodeCycle = (ExecutionNodeCycle) childCycle;
                List<ExecutionTask> childHeadTasks = childNodeCycle.getCycleHeads();
                Map<Integer, ExecutableEvent> needFinishedPrefetchEvents =
                    this.context.getPrefetchEvents();
                for (ExecutionTask childHeadTask : childHeadTasks) {
                    PrefetchEvent prefetchEvent = this.buildPrefetchEvent(childNodeCycle, childHeadTask);
                    ExecutableEvent executableEvent = ExecutableEvent.build(childHeadTask.getWorkerInfo(),
                        childHeadTask, prefetchEvent);
                    iterator.addEvent(executableEvent);
                    needFinishedPrefetchEvents.put(childHeadTask.getTaskId(), executableEvent);
                }
            }
        }
        return iterator;
    }

    private PrefetchEvent buildPrefetchEvent(ExecutionNodeCycle childNodeCycle, ExecutionTask childTask) {
        IoDescriptor ioDescriptor = IoDescriptorBuilder.buildPrefetchIoDescriptor(this.cycle, childNodeCycle, childTask);
        return new PrefetchEvent(
            childNodeCycle.getSchedulerId(),
            childTask.getWorkerInfo().getWorkerIndex(),
            childNodeCycle.getCycleId(),
            this.context.getInitialIterationId(),
            childNodeCycle.getPipelineId(),
            childNodeCycle.getPipelineName(),
            childTask,
            ioDescriptor);
    }

    private IEvent buildInitOrPopEvent(ExecutionTask task, IoDescriptor ioDescriptor) {
        return this.cycle.isWorkerAssigned()
            ? new PopWorkerEvent(
            this.schedulerId,
            task.getWorkerInfo().getWorkerIndex(),
            this.cycle.getCycleId(),
            this.context.getInitialIterationId(),
            this.cycle.getPipelineId(),
            this.cycle.getPipelineName(),
            ioDescriptor,
            task.getTaskId())
            : this.buildInitCycleEvent(task, ioDescriptor);
    }

    private InitCycleEvent buildInitCycleEvent(ExecutionTask task, IoDescriptor ioDescriptor) {
        InitCycleEvent init;
        int workerId = task.getWorkerInfo().getWorkerIndex();
        HighAvailableLevel highAvailableLevel = this.cycle.getHighAvailableLevel();
        if (this.cycle.getType() == ExecutionCycleType.ITERATION) {
            highAvailableLevel = HighAvailableLevel.REDO;
        }

        if (this.cycle instanceof CollectExecutionNodeCycle) {
            init = new InitCollectCycleEvent(
                this.schedulerId,
                workerId,
                this.cycle.getCycleId(),
                this.context.getInitialIterationId(),
                this.cycle.getPipelineId(),
                this.cycle.getPipelineName(),
                ioDescriptor,
                task,
                this.cycle.getDriverId(),
                highAvailableLevel);
        } else {
            init = new InitCycleEvent(
                this.schedulerId,
                workerId,
                this.cycle.getCycleId(),
                this.context.getInitialIterationId(),
                this.cycle.getPipelineId(),
                this.cycle.getPipelineName(),
                ioDescriptor,
                task,
                this.cycle.getDriverId(),
                highAvailableLevel);
        }
        return init;
    }

    private ExecutableEventIterator buildInitIteration(long iterationId) {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (ExecutionTask task : this.cycle.getTasks()) {
            if (ExecutionTaskUtils.isCycleHead(task)) {
                int workerId = task.getWorkerInfo().getWorkerIndex();
                // Load graph.
                IEvent loadGraph = new LoadGraphProcessEvent(this.schedulerId, workerId, cycle.getCycleId(),
                    iterationId, context.getInitialIterationId(), COMPUTE_FETCH_COUNT);
                // Init iteration.
                IoDescriptor ioDescriptor = IoDescriptorBuilder.buildIterationIoDescriptor(
                    task, this.cycle, this.resultManager, OutputType.LOOP);
                InitIterationEvent iterationInit = new InitIterationEvent(
                    this.schedulerId,
                    workerId,
                    this.cycle.getCycleId(),
                    iterationId,
                    this.cycle.getPipelineId(),
                    this.cycle.getPipelineName(),
                    ioDescriptor);
                IEvent execute = new ExecuteFirstIterationEvent(this.schedulerId, workerId, this.cycle.getCycleId(), iterationId);
                ComposeEvent composeEvent = new ComposeEvent(workerId,
                    Arrays.asList(loadGraph, iterationInit, execute));
                iterator.addEvent(task.getWorkerInfo(), task, composeEvent);
            }
        }
        return iterator;
    }

    /**
     * Build launch for all cycle heads.
     */
    private ExecutableEventIterator buildExecute(long iterationId) {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (ExecutionTask task : cycle.getTasks()) {
            if (ExecutionTaskUtils.isCycleHead(task)) {
                // Only submit launch to cycle head.
                long fetchId = iterationId;
                // Fetch previous iteration input.
                if (isIteration) {
                    if (iterationId > DEFAULT_INITIAL_ITERATION_ID) {
                        fetchId = iterationId - 1;
                    } else {
                        fetchId = context.getInitialIterationId();
                    }
                }
                IEvent event = buildExecute(task, task.getWorkerInfo().getWorkerIndex(),
                    cycle.getCycleId(), iterationId, fetchId);
                iterator.addEvent(task.getWorkerInfo(), task, event);
            } else if (iterationId == context.getInitialIterationId()) {
                // Build execute compute for non-tail event during first window.
                int workerId = task.getWorkerInfo().getWorkerIndex();
                ExecuteComputeEvent execute = new ExecuteComputeEvent(
                    this.schedulerId,
                    workerId,
                    cycle.getCycleId(), context.getInitialIterationId(),
                    context.getInitialIterationId(),
                    context.getFinishIterationId() - context.getInitialIterationId() + 1,
                    cycle.getIterationCount() > 1);
                iterator.addEvent(task.getWorkerInfo(), task, execute);

            }
        }
        return iterator;
    }

    private ExecutableEventIterator finishPipeline() {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        boolean needInterrupt = context.getCurrentIterationId() < context.getFinishIterationId();
        for (ExecutionTask task : cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            IEvent cleanEvent;
            if (enableAffinity) {
                cleanEvent = new StashWorkerEvent(this.schedulerId, workerId, cycle.getCycleId(), cycle.getIterationCount(), task.getTaskId());
            } else {
                cleanEvent = new CleanCycleEvent(this.schedulerId, workerId, cycle.getCycleId(), cycle.getIterationCount());
            }
            if (needInterrupt && context.getCycle().getType() != ExecutionCycleType.ITERATION
                && context.getCycle().getType() != ExecutionCycleType.ITERATION_WITH_AGG) {
                InterruptTaskEvent interruptTaskEvent = new InterruptTaskEvent(workerId, cycle.getCycleId());
                ComposeEvent composeEvent =
                    new ComposeEvent(task.getWorkerInfo().getWorkerIndex(),
                        Arrays.asList(interruptTaskEvent, cleanEvent));
                iterator.addEvent(task.getWorkerInfo(), task, composeEvent);
            } else {
                iterator.addEvent(task.getWorkerInfo(), task, cleanEvent);
            }
        }
        return iterator;
    }

    private ExecutableEventIterator finishIteration() {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (ExecutionTask task : this.cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            // Finish iteration
            FinishIterationEvent iterationFinishEvent = new FinishIterationEvent(
                this.schedulerId,
                workerId,
                this.context.getInitialIterationId(),
                this.cycle.getCycleId());

            iterator.addEvent(task.getWorkerInfo(), task, iterationFinishEvent);
        }
        return iterator;
    }

    private ExecutableEventIterator handleRollback() {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        for (ExecutionTask task : this.cycle.getTasks()) {
            int workerId = task.getWorkerInfo().getWorkerIndex();
            // Do not do rollback if recover from initial iteration id.
            if (context.getCurrentIterationId() != DEFAULT_INITIAL_ITERATION_ID) {
                RollbackCycleEvent rollbackCycleEvent = new RollbackCycleEvent(this.schedulerId, workerId,
                    this.cycle.getCycleId(),
                    context.getCurrentIterationId() - 1);
                iterator.addEvent(task.getWorkerInfo(), task, rollbackCycleEvent);
            }
        }
        return iterator;
    }

    private IEvent buildExecute(ExecutionTask task, int workerId, int cycleId, long iterationId, long fetchId) {
        if (cycle.getVertexGroup().getParentVertexGroupIds().isEmpty() && ExecutionTaskUtils.isCycleHead(task)) {
            return new LaunchSourceEvent(this.schedulerId, workerId, cycleId, iterationId);
        } else {
            IoDescriptor ioDescriptor = IoDescriptorBuilder.buildIterationIoDescriptor(
                task, this.cycle, this.resultManager, OutputType.RESPONSE);
            if (ioDescriptor.getInputDescriptor().getInputDescMap().isEmpty()) {
                return new ExecuteComputeEvent(this.schedulerId, workerId, cycleId, iterationId, fetchId, COMPUTE_FETCH_COUNT);
            } else {
                return new IterationExecutionComputeWithAggEvent(
                    this.schedulerId,
                    workerId,
                    cycleId,
                    iterationId,
                    fetchId,
                    COMPUTE_FETCH_COUNT,
                    ioDescriptor);
            }
        }
    }

}
