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

package org.apache.geaflow.runtime.pipeline.runner;

import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.common.ExecutionIdGenerator;
import org.apache.geaflow.cluster.common.IEventListener;
import org.apache.geaflow.cluster.driver.DriverEventDispatcher;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.core.graph.ExecutionGraph;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.builder.ExecutionGraphBuilder;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.context.IPipelineExecutorContext;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.runtime.core.scheduler.CycleSchedulerFactory;
import org.apache.geaflow.runtime.core.scheduler.ExecutionCycleTaskAssigner;
import org.apache.geaflow.runtime.core.scheduler.ExecutionGraphCycleScheduler;
import org.apache.geaflow.runtime.core.scheduler.ICycleScheduler;
import org.apache.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionCycleBuilder;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.result.IExecutionResult;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import org.apache.geaflow.runtime.pipeline.task.PipelineTaskExecutorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRunner.class);

    private DriverEventDispatcher eventDispatcher;
    private ICycleSchedulerContext context;

    public PipelineRunner(DriverEventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    public IExecutionResult executePipelineGraph(IPipelineExecutorContext pipelineExecutorContext,
                                                 PipelineGraph pipelineGraph,
                                                 TaskCallBack taskCallBack) {
        ICycleSchedulerContext context = loadOrCreateContext(pipelineExecutorContext, pipelineGraph);

        if (taskCallBack != null) {
            ((AbstractCycleSchedulerContext) context).setCallbackFunction(taskCallBack.getCallbackFunction());
        }

        ICycleScheduler scheduler = CycleSchedulerFactory.create(context.getCycle());
        if (scheduler instanceof IEventListener) {
            eventDispatcher.registerListener(context.getCycle().getSchedulerId(), (IEventListener) scheduler);
        }

        scheduler.init(context);
        IExecutionResult result = scheduler.execute();
        LOGGER.info("final result of pipeline is {}", result.getResult());
        scheduler.close();
        if (scheduler instanceof IEventListener) {
            eventDispatcher.removeListener(((ExecutionGraphCycleScheduler) scheduler).getSchedulerId());
        }
        return result;
    }

    public void runPipelineGraph(PipelineGraph pipelineGraph, TaskCallBack taskCallBack,
                                 PipelineTaskExecutorContext taskExecutorContext) {
        IExecutionResult result = executePipelineGraph(taskExecutorContext, pipelineGraph, taskCallBack);
        if (!result.isSuccess()) {
            throw new GeaflowRuntimeException("run pipeline task failed, cause: " + result.getError());
        }
    }

    public IExecutionResult runPipelineGraph(PipelineGraph pipelineGraph,
                                             PipelineServiceExecutorContext serviceExecutorContext) {
        //TODO Service task callback.
        return executePipelineGraph(serviceExecutorContext, pipelineGraph, null);
    }

    private ICycleSchedulerContext loadOrCreateContext(IPipelineExecutorContext pipelineExecutorContext,
                                                       PipelineGraph pipelineGraph) {

        ICycleSchedulerContext context = CycleSchedulerContextFactory.loadOrCreate(pipelineExecutorContext.getPipelineTaskId(), () -> {
            ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
            PipelineContext pipelineContext = (PipelineContext) pipelineExecutorContext.getPipelineContext();
            ExecutionGraph graph = builder.buildExecutionGraph(pipelineContext.getConfig());

            Map<Integer, List<ExecutionTask>> vertex2Tasks = ExecutionCycleTaskAssigner.assign(graph);

            // Skip checkpoint if it's a PipelineServiceExecutorContext
            boolean skipCheckpoint = pipelineExecutorContext instanceof PipelineServiceExecutorContext;
            IExecutionCycle cycle = ExecutionCycleBuilder.buildExecutionCycle(graph, vertex2Tasks,
                pipelineContext.getConfig(), ExecutionIdGenerator.getInstance().generateId(),
                pipelineExecutorContext.getPipelineTaskId(), pipelineExecutorContext.getPipelineTaskName(),
                ExecutionIdGenerator.getInstance().generateId(), pipelineExecutorContext.getDriverId(),
                pipelineExecutorContext.getDriverIndex(), skipCheckpoint);
            return CycleSchedulerContextFactory.create(cycle, null);
        });
        return context;
    }
}
