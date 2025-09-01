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

package org.apache.geaflow.runtime.pipeline.task;

import java.io.Serializable;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.plan.PipelinePlanBuilder;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.runtime.pipeline.executor.PipelineExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineTaskExecutor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutor.class);

    private PipelineTaskExecutorContext taskExecutorContext;

    public PipelineTaskExecutor(PipelineTaskExecutorContext taskExecutorContext) {
        this.taskExecutorContext = taskExecutorContext;
    }

    public void execute(PipelineTask pipelineTask, TaskCallBack taskCallBack) {
        // User pipeline Task.
        PipelineTaskContext taskContext = new PipelineTaskContext(taskExecutorContext.getPipelineTaskId(),
            taskExecutorContext.getPipelineContext());
        pipelineTask.execute(taskContext);

        PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
        // 1. Build pipeline graph plan.
        PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(taskExecutorContext.getPipelineContext());

        // 2. Optimize pipeline graph plan.
        pipelinePlanBuilder.optimizePlan(taskExecutorContext.getPipelineContext().getConfig());

        this.taskExecutorContext.getPipelineRunner().runPipelineGraph(pipelineGraph, taskCallBack
            , taskExecutorContext);
    }
}
