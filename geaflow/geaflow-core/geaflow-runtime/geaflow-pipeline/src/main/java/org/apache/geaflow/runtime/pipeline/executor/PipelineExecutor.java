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

package org.apache.geaflow.runtime.pipeline.executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.executor.IPipelineExecutor;
import org.apache.geaflow.cluster.executor.PipelineExecutorContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.PipelineTaskType;
import org.apache.geaflow.runtime.pipeline.runner.PipelineRunner;
import org.apache.geaflow.runtime.pipeline.service.PipelineServiceExecutor;
import org.apache.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import org.apache.geaflow.runtime.pipeline.task.PipelineTaskExecutor;
import org.apache.geaflow.runtime.pipeline.task.PipelineTaskExecutorContext;
import org.apache.geaflow.view.IViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineExecutor implements IPipelineExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutor.class);
    private PipelineRunner pipelineRunner;
    private PipelineExecutorContext executorContext;
    private List<IViewDesc> viewDescList;
    private Map<PipelineService, PipelineServiceExecutor> serviceExecutorMap;

    public void init(PipelineExecutorContext executorContext) {
        this.executorContext = executorContext;
        this.pipelineRunner = new PipelineRunner(executorContext.getEventDispatcher());
        this.serviceExecutorMap = new HashMap<>();
    }

    @Override
    public void register(List<IViewDesc> viewDescList) {
        this.viewDescList = viewDescList;
    }

    @Override
    public void runPipelineTask(PipelineTask pipelineTask, TaskCallBack taskCallBack) {
        int pipelineTaskId = executorContext.getIdGenerator().getAndIncrement();
        String pipelineTaskName = String.format("%s#%s", PipelineTaskType.PipelineTask.name(),
            executorContext.getIdGenerator().getAndIncrement());
        LOGGER.info("run pipeline task {}", pipelineTaskName);

        PipelineContext pipelineContext = new PipelineContext(PipelineTaskType.PipelineTask.name(),
            executorContext.getEnvConfig());
        this.viewDescList.stream().forEach(viewDesc -> pipelineContext.addView(viewDesc));

        PipelineTaskExecutorContext taskExecutorContext =
            new PipelineTaskExecutorContext(executorContext.getDriverId(),
                pipelineTaskId, pipelineTaskName, pipelineContext, pipelineRunner);
        PipelineTaskExecutor taskExecutor = new PipelineTaskExecutor(taskExecutorContext);
        taskExecutor.execute(pipelineTask, taskCallBack);
    }

    @Override
    public void startPipelineService(PipelineService pipelineService) {
        int pipelineTaskId = executorContext.getIdGenerator().getAndIncrement();
        String pipelineTaskName = String.format("%s#%s", PipelineTaskType.PipelineTask.name(), pipelineTaskId);
        LOGGER.info("run pipeline task {}", pipelineTaskName);

        Configuration configuration = new Configuration();
        configuration.putAll(executorContext.getEnvConfig().getConfigMap());
        configuration.setMasterId(executorContext.getEnvConfig().getMasterId());
        PipelineContext pipelineContext = new PipelineContext(PipelineTaskType.PipelineTask.name(),
            configuration);
        this.viewDescList.stream().forEach(viewDesc -> pipelineContext.addView(viewDesc));

        PipelineServiceExecutorContext pipelineServiceExecutorContext =
            new PipelineServiceExecutorContext(executorContext.getDriverId(), executorContext.getDriverIndex(),
                pipelineTaskId, pipelineTaskName, pipelineContext, pipelineRunner, pipelineService);
        PipelineServiceExecutor serviceExecutor =
            new PipelineServiceExecutor(pipelineServiceExecutorContext);
        serviceExecutorMap.put(pipelineService, serviceExecutor);
        serviceExecutor.start();
    }

    @Override
    public void stopPipelineService(PipelineService pipelineService) {
        serviceExecutorMap.get(pipelineService).stop();
        LOGGER.info("stopped pipeline service {}", pipelineService);
    }
}
