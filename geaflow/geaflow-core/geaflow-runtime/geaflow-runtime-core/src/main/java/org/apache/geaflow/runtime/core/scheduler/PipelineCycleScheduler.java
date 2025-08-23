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

import static org.apache.geaflow.runtime.core.scheduler.io.IoDescriptorBuilder.COLLECT_DATA_EDGE_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.geaflow.cluster.common.IEventListener;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.protocol.ScheduleStateType;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.response.IResult;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.metric.CycleMetrics;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.utils.FutureUtil;
import org.apache.geaflow.common.utils.LoggerFormatter;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.runtime.core.protocol.ComposeEvent;
import org.apache.geaflow.runtime.core.protocol.DoneEvent;
import org.apache.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.io.CycleResultManager;
import org.apache.geaflow.runtime.core.scheduler.response.EventListenerKey;
import org.apache.geaflow.runtime.core.scheduler.response.SourceFinishResponseEventListener;
import org.apache.geaflow.runtime.core.scheduler.statemachine.ComposeState;
import org.apache.geaflow.runtime.core.scheduler.statemachine.IScheduleState;
import org.apache.geaflow.runtime.core.scheduler.statemachine.pipeline.PipelineStateMachine;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is pipeline cycle scheduler impl.
 */
public class PipelineCycleScheduler<P extends ICycleSchedulerContext<ExecutionGraphCycle, ?, ?>, E>
    extends AbstractCycleScheduler<ExecutionNodeCycle, ExecutionGraphCycle, P, List<IResult>, E> implements IEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineCycleScheduler.class);

    private ExecutionNodeCycle nodeCycle;
    private CycleResultManager resultManager;

    private CycleResponseEventPool<IEvent> responseEventPool;
    private HashMap<Long, List<IEvent>> iterationIdToFinishedTasks;
    private Map<Integer, EventMetrics[]> vertexIdToMetrics;
    private Map<Integer, ExecutionTask> cycleTasks;
    private String pipelineName;
    private long pipelineId;
    private int cycleId;
    private long schedulerId;
    private long scheduleStartTime;
    private boolean isIteration;

    private SchedulerEventBuilder eventBuilder;
    private SchedulerGraphAggregateProcessor aggregator;
    private String cycleLogTag;

    public PipelineCycleScheduler() {
    }

    public PipelineCycleScheduler(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    @Override
    public void init(ICycleSchedulerContext<ExecutionNodeCycle, ExecutionGraphCycle, P> context) {
        super.init(context);
        this.responseEventPool = new CycleResponseEventPool<>();
        this.iterationIdToFinishedTasks = new HashMap<>();
        this.nodeCycle = context.getCycle();
        this.cycleTasks = nodeCycle.getTasks().stream().collect(Collectors.toMap(ExecutionTask::getTaskId, t -> t));
        this.isIteration = nodeCycle.getVertexGroup().getCycleGroupMeta().isIterative();
        this.pipelineName = nodeCycle.getPipelineName();
        this.pipelineId = nodeCycle.getPipelineId();
        this.cycleId = nodeCycle.getCycleId();
        this.resultManager = context.getResultManager();
        this.cycleLogTag = LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId);
        this.dispatcher = new SchedulerEventDispatcher(cycleLogTag);
        this.initMetrics();

        this.stateMachine = new PipelineStateMachine();
        this.stateMachine.init(context);

        this.eventBuilder = new SchedulerEventBuilder(context, this.resultManager, this.schedulerId);
        if (nodeCycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG) {
            this.aggregator = new SchedulerGraphAggregateProcessor(nodeCycle,
                (AbstractCycleSchedulerContext) context, resultManager);
        }

        registerEventListener();
    }

    private void initMetrics() {
        this.scheduleStartTime = System.currentTimeMillis();
        this.vertexIdToMetrics = new HashMap<>();
        Map<Integer, ExecutionVertex> vertexMap = this.nodeCycle.getVertexGroup().getVertexMap();
        for (Map.Entry<Integer, ExecutionVertex> entry : vertexMap.entrySet()) {
            Integer vertexId = entry.getKey();
            ExecutionVertex vertex = entry.getValue();
            this.vertexIdToMetrics.put(vertexId, new EventMetrics[vertex.getParallelism()]);
        }
    }

    @Override
    public void close() {
        super.close();
        iterationIdToFinishedTasks.clear();
        responseEventPool.clear();
        if (context.getParentContext() == null) {
            ShuffleManager.getInstance().release(pipelineId);
            ShuffleManager.getInstance().close();
        }
        if (context.getParentContext() == null) {
            context.close(cycle);
        }
        LOGGER.info("{} closed", cycleLogTag);
    }

    public long getSchedulerId() {
        return schedulerId;
    }

    public void setSchedulerId(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    @Override
    protected void execute(IScheduleState state) {
        ExecutableEventIterator iterator = new ExecutableEventIterator();
        if (state.getScheduleStateType() == ScheduleStateType.COMPOSE) {
            for (IScheduleState s : ((ComposeState) state).getStates()) {
                getNextIterationId(context, s);
                ExecutableEventIterator tmp = this.eventBuilder.build(s.getScheduleStateType(), this.context.getCurrentIterationId());
                iterator.merge(tmp);
            }
        } else {
            getNextIterationId(context, state);
            ExecutableEventIterator tmp = this.eventBuilder.build(state.getScheduleStateType(), this.context.getCurrentIterationId());
            iterator.merge(tmp);
        }
        iterator.markReady();

        String iterationLogTag = getCycleIterationTag(this.context.getCurrentIterationId());
        LOGGER.info("{} execute", iterationLogTag);
        this.iterationIdToFinishedTasks.put(this.context.getCurrentIterationId(), new ArrayList<>());
        this.executeEvents(iterator, this.context.getCurrentIterationId());
    }

    private void executeEvents(ExecutableEventIterator eventIterator, long iterationId) {
        List<Future<IEvent>> submitFutures = new ArrayList<>(eventIterator.size());
        while (eventIterator.hasNext()) {
            Tuple<WorkerInfo, List<ExecutableEventIterator.ExecutableEvent>> tuple = eventIterator.next();
            WorkerInfo worker = tuple.f0;
            List<ExecutableEventIterator.ExecutableEvent> executableEvents = tuple.f1;
            for (ExecutableEventIterator.ExecutableEvent executableEvent : executableEvents) {
                ExecutionTask task = executableEvent.getTask();
                IEvent event = executableEvent.getEvent();
                String taskTag = this.isIteration
                    ? LoggerFormatter.getTaskTag(this.pipelineName, this.cycleId, iterationId,
                    task.getTaskId(), task.getVertexId(), task.getIndex(), task.getParallelism())
                    : LoggerFormatter.getTaskTag(this.pipelineName, this.cycleId, task.getTaskId(),
                    task.getVertexId(), task.getIndex(), task.getParallelism());
                LOGGER.info("{} submit event {} on host {} {} process {}",
                    taskTag,
                    event,
                    worker.getHost(),
                    worker.getWorkerIndex(),
                    worker.getProcessId());
            }
            IEvent finalEvent;
            if (executableEvents.size() == 1) {
                finalEvent = executableEvents.get(0).getEvent();
            } else {
                List<IEvent> events = executableEvents.stream()
                    .map(ExecutableEventIterator.ExecutableEvent::getEvent)
                    .collect(Collectors.toList());
                finalEvent = new ComposeEvent(worker.getWorkerIndex(), flatEvents(events));
            }
            Future<IEvent> future = (Future<IEvent>) RpcClient.getInstance()
                .processContainer(worker.getContainerName(), finalEvent);
            submitFutures.add(future);
        }
        FutureUtil.wait(submitFutures);
    }

    private static List<IEvent> flatEvents(List<IEvent> list) {
        List<IEvent> events = new ArrayList<>();
        for (IEvent event : list) {
            if (event.getEventType() == EventType.COMPOSE) {
                events.addAll(flatEvents(((ComposeEvent) event).getEventList()));
            } else {
                events.add(event);
            }
        }
        return events;
    }

    protected void finish(long iterationId) {
        String iterationLogTag = getCycleIterationTag(iterationId);
        if (iterationIdToFinishedTasks.get(iterationId) == null) {
            // Unexpected to reach here.
            throw new GeaflowRuntimeException(String.format("fatal: %s result is unregistered",
                iterationLogTag));
        }

        int expectedResponseSize = nodeCycle.getCycleTails().size();
        while (iterationIdToFinishedTasks.get(iterationId).size() != expectedResponseSize) {
            IEvent response = responseEventPool.waitEvent();
            DoneEvent event = (DoneEvent) response;
            // Get iterationId from task.
            long currentTaskIterationId = event.getWindowId();
            if (!iterationIdToFinishedTasks.containsKey(currentTaskIterationId)) {
                throw new GeaflowRuntimeException(
                    String.format("%s finish error, current response iterationId %s, current waiting iterationIds %s",
                        cycleLogTag,
                        currentTaskIterationId,
                        iterationIdToFinishedTasks.keySet()));
            }
            iterationIdToFinishedTasks.get(currentTaskIterationId).add(response);
        }

        // Get current iteration result.
        List<IEvent> responses = iterationIdToFinishedTasks.remove(iterationId);
        for (IEvent e : responses) {
            registerResults((DoneEvent) e);
        }

        if (this.isIteration) {
            this.collectEventMetrics(responses, iterationId);
        }
        LOGGER.info("{} finished iterationId {}", iterationLogTag, iterationId);
    }

    protected List<IResult> finish() {
        long finishIterationId = this.isIteration
            ? this.context.getFinishIterationId() + 1 : this.context.getFinishIterationId();
        String finishLogTag = this.getCycleIterationTag(finishIterationId);

        // Need receive all tail responses.
        int responseCount = 0;

        List<IEvent> resultResponses = new ArrayList<>(this.cycleTasks.size());
        List<IEvent> metricResponses = new ArrayList<>(this.cycleTasks.size());
        while (true) {
            IEvent e = responseEventPool.waitEvent();
            DoneEvent<List<IResult>> event = (DoneEvent) e;
            switch (event.getSourceEvent()) {
                case EXECUTE_COMPUTE:
                    resultResponses.add(event);
                    break;
                default:
                    metricResponses.add(event);
                    responseCount++;
                    break;
            }
            if (responseCount == cycleTasks.size()) {
                LOGGER.info("{} all task result collected", finishLogTag);
                break;
            }
        }
        if (!resultResponses.isEmpty()) {
            for (IEvent e : resultResponses) {
                registerResults((DoneEvent) e);
            }
        }
        if (!metricResponses.isEmpty()) {
            this.collectEventMetrics(metricResponses, finishIterationId);
            LOGGER.info("{} finished", finishLogTag);
        }

        return context.getResultManager().getDataResponse();
    }

    @Override
    public void handleEvent(IEvent event) {
        LOGGER.info("{} handle event {}", cycleLogTag, event);
        if (event.getEventType() == EventType.COMPOSE) {
            for (IEvent e : ((ComposeEvent) event).getEventList()) {
                handleEvent(e);
            }
        } else {
            dispatcher.dispatch(event);
        }
    }

    private void registerResults(DoneEvent<Map<Integer, IResult>> event) {

        if (event.getResult() != null) {
            // Register result to resultManager.
            for (IResult result : event.getResult().values()) {
                LOGGER.info("{} register result for {}", event, result.getId());
                if (result.getType() == OutputType.RESPONSE && result.getId() != COLLECT_DATA_EDGE_ID) {
                    LOGGER.info("do aggregate, result {}", result.getResponse());
                    aggregator.aggregate(result.getResponse());
                } else {
                    resultManager.register(result.getId(), result);
                }
            }
        }
    }

    private void collectEventMetrics(List<IEvent> responses, long windowId) {
        Map<Integer, List<EventMetrics>> vertexId2metrics = responses.stream()
            .map(e -> ((DoneEvent<?>) e).getEventMetrics())
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(EventMetrics::getVertexId));

        long duration = System.currentTimeMillis() - this.scheduleStartTime;
        for (Map.Entry<Integer, List<EventMetrics>> entry : vertexId2metrics.entrySet()) {
            Integer vertexId = entry.getKey();
            List<EventMetrics> metrics = entry.getValue();
            EventMetrics[] previousMetrics = this.vertexIdToMetrics.get(vertexId);

            int taskNum = previousMetrics.length;
            int slowestTask = 0;
            long executeCostMs = 0;
            long totalExecuteTime = 0;
            long totalGcTime = 0;
            long slowestTaskExecuteTime = 0;
            long totalInputRecords = 0;
            long totalInputBytes = 0;
            long totalOutputRecords = 0;
            long totalOutputBytes = 0;

            for (EventMetrics eventMetrics : metrics) {
                int index = eventMetrics.getIndex();
                EventMetrics previous = previousMetrics[index];
                if (previous == null) {
                    executeCostMs = eventMetrics.getProcessCostMs();
                    totalExecuteTime += executeCostMs;
                    totalGcTime += eventMetrics.getGcCostMs();
                    totalInputRecords += eventMetrics.getShuffleReadRecords();
                    totalInputBytes += eventMetrics.getShuffleReadBytes();
                    totalOutputRecords += eventMetrics.getShuffleWriteRecords();
                    totalOutputBytes += eventMetrics.getShuffleWriteBytes();
                } else {
                    executeCostMs = eventMetrics.getProcessCostMs() - previous.getProcessCostMs();
                    totalExecuteTime += executeCostMs;
                    totalGcTime += eventMetrics.getGcCostMs() - previous.getGcCostMs();
                    totalInputRecords += eventMetrics.getShuffleReadRecords() - previous.getShuffleReadRecords();
                    totalInputBytes += eventMetrics.getShuffleReadBytes() - previous.getShuffleReadBytes();
                    totalOutputRecords += eventMetrics.getShuffleWriteRecords() - previous.getShuffleWriteRecords();
                    totalOutputBytes += eventMetrics.getShuffleWriteBytes() - previous.getShuffleWriteBytes();
                }
                if (executeCostMs > slowestTaskExecuteTime) {
                    slowestTaskExecuteTime = executeCostMs;
                    slowestTask = index;
                }
                if (this.isIteration) {
                    previousMetrics[index] = eventMetrics;
                }
            }

            String metricName = this.isIteration
                ? LoggerFormatter.getCycleMetricName(this.cycleId, windowId, vertexId)
                : LoggerFormatter.getCycleMetricName(this.cycleId, vertexId);
            String opName = this.nodeCycle.getVertexGroup().getVertexMap().get(vertexId).getName();
            CycleMetrics cycleMetrics = CycleMetrics.build(
                metricName,
                this.pipelineName,
                opName,
                taskNum,
                slowestTask,
                this.scheduleStartTime,
                duration,
                totalExecuteTime,
                totalGcTime,
                slowestTaskExecuteTime,
                totalInputRecords,
                totalInputBytes,
                totalOutputRecords,
                totalOutputBytes
            );
            LOGGER.info("collect metric {} {}", metricName, cycleMetrics);
            StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportCycleMetrics(cycleMetrics);
        }

        this.scheduleStartTime = System.currentTimeMillis();
    }

    private void getNextIterationId(ICycleSchedulerContext<?, ?, ?> context, IScheduleState state) {
        if (state.getScheduleStateType() == ScheduleStateType.EXECUTE_COMPUTE
            || state.getScheduleStateType() == ScheduleStateType.ITERATION_INIT) {
            context.getNextIterationId();
        }
    }

    private String getCycleIterationTag(long iterationId) {
        return this.isIteration
            ? LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId, iterationId)
            : LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId);
    }

    @Override
    protected void registerEventListener() {
        registerSourceFinishEventListener();
        registerResponseEventListener();
    }

    private void registerSourceFinishEventListener() {
        EventListenerKey listenerKey = EventListenerKey.of(cycle.getCycleId(), EventType.LAUNCH_SOURCE);
        IEventListener listener =
            new SourceFinishResponseEventListener(nodeCycle.getCycleHeads().size(),
                events -> {
                    long sourceFinishWindowId =
                        events.stream().map(e -> ((DoneEvent) e).getWindowId()).max(Long::compareTo).get();
                    ((AbstractCycleSchedulerContext) context)
                        .setTerminateIterationId(sourceFinishWindowId);
                    LOGGER.info("{} all source finished at {}", cycleLogTag, sourceFinishWindowId);


                    ICycleSchedulerContext parentContext = ((AbstractCycleSchedulerContext) context).getParentContext();
                    if (parentContext != null) {
                        DoneEvent sourceFinishEvent = new DoneEvent(schedulerId,
                            parentContext.getCycle().getCycleId(),
                            sourceFinishWindowId, cycle.getCycleId(),
                            EventType.LAUNCH_SOURCE, cycle.getCycleId());
                        RpcClient.getInstance().processPipeline(cycle.getDriverId(), sourceFinishEvent);
                    }
                });

        // Register listener for end of source event.
        this.dispatcher.registerListener(listenerKey, listener);
    }

    private void registerResponseEventListener() {
        EventListenerKey listenerKey = EventListenerKey.of(cycle.getCycleId());
        IEventListener listener = new ResponseEventListener();
        this.dispatcher.registerListener(listenerKey, listener);
    }

    public class ResponseEventListener implements IEventListener {

        @Override
        public void handleEvent(IEvent event) {
            responseEventPool.notifyEvent(event);
        }
    }

}
