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

package org.apache.geaflow.cluster.executor;

import java.util.List;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.IViewDesc;

public interface IPipelineExecutor {

    /**
     * Init pipeline executor.
     */
    void init(PipelineExecutorContext executorContext);

    /**
     * Register view desc list.
     */
    void register(List<IViewDesc> viewDescList);

    /**
     * Trigger to run pipeline task.
     */
    void runPipelineTask(PipelineTask pipelineTask, TaskCallBack taskCallBack);

    /**
     * Trigger to start pipeline service.
     */
    void startPipelineService(PipelineService pipelineService);

    /**
     * Stop pipeline service server.
     */
    void stopPipelineService(PipelineService pipelineService);
}
