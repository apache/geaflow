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

package org.apache.geaflow.runtime.pipeline.service;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.pipeline.service.IPipelineServiceExecutorContext;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.runner.PipelineRunner;

public class PipelineServiceExecutorContext implements IPipelineServiceExecutorContext {

    private String driverId;
    private int driverIndex;
    private long pipelineTaskId;
    private String pipelineTaskName;
    private PipelineContext pipelineContext;
    private PipelineRunner pipelineRunner;
    private PipelineService pipelineService;

    public PipelineServiceExecutorContext(String driverId,
                                          int driverIndex,
                                          long pipelineTaskId,
                                          String pipelineTaskName,
                                          PipelineContext pipelineContext,
                                          PipelineRunner pipelineRunner,
                                          PipelineService pipelineService) {
        this.driverId = driverId;
        this.driverIndex = driverIndex;
        this.pipelineTaskId = pipelineTaskId;
        this.pipelineTaskName = pipelineTaskName;
        this.pipelineContext = pipelineContext;
        this.pipelineRunner = pipelineRunner;
        this.pipelineService = pipelineService;
    }

    public String getDriverId() {
        return driverId;
    }

    public int getDriverIndex() {
        return driverIndex;
    }

    public long getPipelineTaskId() {
        return pipelineTaskId;
    }

    public String getPipelineTaskName() {
        return pipelineTaskName;
    }

    public PipelineContext getPipelineContext() {
        return pipelineContext;
    }

    public PipelineRunner getPipelineRunner() {
        return pipelineRunner;
    }

    @Override
    public Configuration getConfiguration() {
        return pipelineContext.getConfig();
    }

    @Override
    public PipelineService getPipelineService() {
        return pipelineService;
    }
}
