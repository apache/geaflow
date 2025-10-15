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

package org.apache.geaflow.pipeline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.IViewDesc;

public class Pipeline implements Serializable {

    private transient Environment environment;
    private List<PipelineTask> pipelineTaskList;
    private List<TaskCallBack> pipelineTaskCallbacks;
    private List<PipelineService> pipelineServices;
    private Map<String, IViewDesc> viewDescMap;

    public Pipeline(Environment environment) {
        this.environment = environment;
        this.environment.addPipeline(this);
        this.viewDescMap = new HashMap<>();
        this.pipelineTaskList = new ArrayList<>();
        this.pipelineTaskCallbacks = new ArrayList<>();
        this.pipelineServices = new ArrayList<>();
    }

    public void init() {

    }

    public Pipeline withView(String viewName, IViewDesc viewDesc) {
        this.viewDescMap.put(viewName, viewDesc);
        return this;
    }

    public TaskCallBack submit(PipelineTask pipelineTask) {
        this.pipelineTaskList.add(pipelineTask);
        TaskCallBack taskCallBack = new TaskCallBack();
        this.pipelineTaskCallbacks.add(taskCallBack);
        return taskCallBack;
    }

    public Pipeline start(PipelineService pipelineService) {
        this.pipelineServices.add(pipelineService);
        return this;
    }

    public Pipeline schedule(PipelineTask pipelineTask) {
        this.pipelineTaskList.add(pipelineTask);
        return this;
    }

    public IPipelineResult execute() {
        this.environment.init();
        return this.environment.submit();
    }


    public void shutdown() {
        this.environment.shutdown();
    }

    public List<IViewDesc> getViewDescMap() {
        return viewDescMap.values().stream().collect(Collectors.toList());
    }

    public List<PipelineTask> getPipelineTaskList() {
        return pipelineTaskList;
    }

    public List<TaskCallBack> getPipelineTaskCallbacks() {
        return pipelineTaskCallbacks;
    }

    public List<PipelineService> getPipelineServices() {
        return pipelineServices;
    }
}
