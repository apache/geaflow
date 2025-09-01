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

package org.apache.geaflow.runtime.core.scheduler.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.cluster.client.utils.PipelineUtil;
import org.apache.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.RequireResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.RequireResponse;
import org.apache.geaflow.cluster.resourcemanager.ResourceInfo;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.resourcemanager.allocator.IAllocator;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import org.apache.geaflow.plan.graph.AffinityLevel;
import org.apache.geaflow.processor.impl.AbstractProcessor;
import org.apache.geaflow.rpc.proto.Container;
import org.apache.geaflow.runtime.core.protocol.ComposeEvent;
import org.apache.geaflow.runtime.core.protocol.CreateTaskEvent;
import org.apache.geaflow.runtime.core.protocol.CreateWorkerEvent;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractScheduledWorkerManager implements IScheduledWorkerManager<ExecutionGraphCycle> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScheduledWorkerManager.class);
    private static final String SEPARATOR = "#";
    protected static final String DEFAULT_RESOURCE_ID = "default_";
    protected static final String DEFAULT_GRAPH_VIEW_NAME = "default_graph_view_name";

    protected static final int RETRY_REQUEST_RESOURCE_INTERVAL = 5;
    protected static final int REPORT_RETRY_TIMES = 50;

    protected final String masterId;
    protected final boolean isAsync;
    protected transient Map<Long, ResourceInfo> workers;
    protected transient Map<Long, List<ExecutionNodeCycle>> nodeCycles;
    protected transient Map<Long, Boolean> isAssigned;
    protected TaskAssigner taskAssigner;

    public AbstractScheduledWorkerManager(Configuration config) {
        this.masterId = config.getMasterId();
        this.isAsync = PipelineUtil.isAsync(config);
        if (this.taskAssigner == null) {
            this.taskAssigner = new TaskAssigner();
        }
    }

    @Override
    public void init(ExecutionGraphCycle graph) {
        if (this.workers == null) {
            this.workers = new ConcurrentHashMap<>();
            this.nodeCycles = new ConcurrentHashMap<>();
            this.isAssigned = new ConcurrentHashMap<>();
        }
        Long schedulerId = graph.getSchedulerId();
        String resourceId = this.genResourceId(graph.getDriverIndex(), schedulerId);

        int requestResourceNum = graph.getCycleMap().values().stream()
            .map(e -> ((ExecutionNodeCycle) e).getVertexGroup())
            .map(AbstractScheduledWorkerManager::getExecutionGroupParallelism)
            .max(Integer::compareTo)
            .orElse(0);

        List<WorkerInfo> allocated = this.requestWorker(requestResourceNum, resourceId);
        this.initWorkers(schedulerId, allocated, ScheduledWorkerManagerFactory.getWorkerManagerHALevel(graph));
        this.workers.put(schedulerId, new ResourceInfo(resourceId, allocated));
        LOGGER.info("scheduler {} request {} workers from resource manager", schedulerId, allocated.size());

        this.nodeCycles.put(schedulerId, extractNodeCycles(graph));
    }

    private static List<ExecutionNodeCycle> extractNodeCycles(ExecutionGraphCycle graph) {
        List<ExecutionNodeCycle> list = new ArrayList<>(graph.getCycleMap().size());
        for (IExecutionCycle cycle : graph.getCycleMap().values()) {
            if (cycle instanceof ExecutionNodeCycle) {
                list.add((ExecutionNodeCycle) cycle);
            }
        }
        return list;
    }

    @Override
    public List<WorkerInfo> assign(ExecutionGraphCycle graph) {
        Long schedulerId = graph.getSchedulerId();
        boolean assigned = Optional.ofNullable(this.isAssigned.get(schedulerId)).orElse(false);
        List<WorkerInfo> allocated = this.workers.get(schedulerId).getWorkers();
        List<Integer> taskIndexes = new ArrayList<>();
        for (int i = 0; i < allocated.size(); i++) {
            taskIndexes.add(i);
        }
        String graphName = getGraphViewName(graph);
        Map<Integer, WorkerInfo> taskIndex2Worker = taskAssigner.assignTasks2Workers(graphName,
            taskIndexes, allocated);
        for (IExecutionCycle cycle : graph.getCycleMap().values()) {
            if (cycle instanceof ExecutionNodeCycle) {
                ExecutionNodeCycle nodeCycle = (ExecutionNodeCycle) cycle;
                int parallelism = getExecutionGroupParallelism(nodeCycle.getVertexGroup());
                for (int i = 0; i < parallelism; i++) {
                    ExecutionTask task = nodeCycle.getTasks().get(i);
                    task.setWorkerInfo(taskIndex2Worker.get(i));
                }
                nodeCycle.setWorkerAssigned(assigned);
            }
        }
        return allocated;
    }

    protected WorkerInfo assignTaskWorker(WorkerInfo worker, ExecutionTask task, AffinityLevel affinityLevel) {
        return worker;
    }

    @Override
    public void release(ExecutionGraphCycle graph) {
        Long schedulerId = graph.getSchedulerId();
        ResourceInfo remove = this.workers.remove(schedulerId);
        if (remove != null) {
            RpcClient.getInstance().releaseResource(masterId,
                ReleaseResourceRequest.build(remove.getResourceId(), remove.getWorkers()));
        }
    }

    @Override
    public void clean(CleanWorkerFunction function, IExecutionCycle cycle) {
        Long schedulerId = cycle.getSchedulerId();
        function.clean(this.workers.get(schedulerId).getWorkers());
        this.isAssigned.put(schedulerId, true);
        for (ExecutionNodeCycle nodeCycle : this.nodeCycles.get(schedulerId)) {
            nodeCycle.setWorkerAssigned(true);
        }
    }

    @Override
    public synchronized void close(IExecutionCycle cycle) {
        if (this.isAssigned != null) {
            this.isAssigned.remove(cycle.getSchedulerId());
        }
    }

    protected static int getExecutionGroupParallelism(ExecutionVertexGroup vertexGroup) {
        return vertexGroup.getVertexMap().values().stream()
            .map(ExecutionVertex::getParallelism)
            .reduce(Integer::sum)
            .orElse(0);
    }

    protected List<WorkerInfo> requestWorker(int requestResourceNum, String resourceId) {
        IAllocator.AllocateStrategy allocateStrategy = this.isAsync
            ? IAllocator.AllocateStrategy.PROCESS_FAIR : IAllocator.AllocateStrategy.ROUND_ROBIN;
        RequireResponse response = RpcClient.getInstance().requireResource(this.masterId,
            RequireResourceRequest.build(resourceId, requestResourceNum, allocateStrategy));
        int retryTimes = 1;
        while (!response.isSuccess() || response.getWorkers().isEmpty()) {
            try {
                response = RpcClient.getInstance().requireResource(masterId,
                    RequireResourceRequest.build(resourceId,
                        requestResourceNum,
                        allocateStrategy));
                if (retryTimes % REPORT_RETRY_TIMES == 0) {
                    String msg = String.format("request %s worker with allocateStrategy %s failed after %s times: %s",
                        requestResourceNum, allocateStrategy, retryTimes, response.getMsg());
                    LOGGER.warn(msg);
                }
                Thread.sleep((long) RETRY_REQUEST_RESOURCE_INTERVAL * retryTimes);
                // TODO Report to ExceptionCollector.
                retryTimes++;
            } catch (InterruptedException e) {
                throw new GeaflowRuntimeException(e);
            }
        }
        return response.getWorkers();
    }

    protected void initWorkers(Long schedulerId, List<WorkerInfo> workers, HighAvailableLevel highAvailableLevel) {
        if (this.workers.get(schedulerId) != null) {
            LOGGER.info("recovered workers {} already init, ignore init again", workers.size());
            return;
        }
        LOGGER.info("do init workers {}", workers.size());
        CountDownLatch processCountDownLatch = new CountDownLatch(workers.size());
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        for (WorkerInfo workerInfo : workers) {
            int workerId = workerInfo.getWorkerIndex();
            CreateTaskEvent createTaskEvent = new CreateTaskEvent(workerId, highAvailableLevel);
            CreateWorkerEvent createWorkerEvent = new CreateWorkerEvent(workerId, highAvailableLevel);
            ComposeEvent composeEvent = new ComposeEvent(workerId, Arrays.asList(createTaskEvent, createWorkerEvent));

            RpcClient.getInstance().processContainer(workerInfo.getContainerName(), composeEvent,
                new RpcEndpointRef.RpcCallback<Container.Response>() {
                    @Override
                    public void onSuccess(Container.Response value) {
                        processCountDownLatch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        processCountDownLatch.countDown();
                        failureCount.incrementAndGet();
                        exception.compareAndSet(null, t);
                    }
                });
        }
        try {
            processCountDownLatch.await();
            LOGGER.info("do init workers finished");
            if (failureCount.get() > 0) {
                throw new GeaflowRuntimeException(String.format("init worker failed. failed count %s",
                    failureCount.get()), exception.get());
            }
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public String genResourceId(int driverIndex, Long schedulerId) {
        return DEFAULT_RESOURCE_ID + driverIndex + SEPARATOR + schedulerId;
    }

    private String getGraphViewName(ExecutionGraphCycle graph) {
        for (IExecutionCycle cycle : graph.getCycleMap().values()) {
            if (cycle instanceof ExecutionNodeCycle) {
                ExecutionNodeCycle nodeCycle = (ExecutionNodeCycle) cycle;
                List<ExecutionTask> tasks = nodeCycle.getTasks();
                for (ExecutionTask task : tasks) {
                    AbstractProcessor processor = (AbstractProcessor) (task.getProcessor());
                    if (processor != null) {
                        Operator operator = processor.getOperator();
                        if (operator instanceof AbstractGraphVertexCentricOp) {
                            AbstractGraphVertexCentricOp graphOp =
                                (AbstractGraphVertexCentricOp) operator;
                            return graphOp.getGraphViewName();
                        }
                    }
                }

            }
        }
        return DEFAULT_GRAPH_VIEW_NAME;
    }
}