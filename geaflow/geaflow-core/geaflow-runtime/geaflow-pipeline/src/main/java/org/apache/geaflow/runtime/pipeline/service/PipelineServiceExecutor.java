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

import java.io.Serializable;
import org.apache.geaflow.pipeline.service.IServiceServer;
import org.apache.geaflow.runtime.pipeline.executor.PipelineExecutor;
import org.apache.geaflow.runtime.pipeline.service.util.ServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineServiceExecutor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutor.class);

    private PipelineServiceExecutorContext serviceExecutorContext;
    private IServiceServer serviceServer;

    public PipelineServiceExecutor(PipelineServiceExecutorContext serviceExecutorContext) {
        this.serviceExecutorContext = serviceExecutorContext;
    }

    public void start() {
        LOGGER.info("start pipeline service {}", serviceExecutorContext.getPipelineService());
        this.serviceServer = ServerFactory.loadServer(this.serviceExecutorContext);
        this.serviceServer.startServer();
    }

    public void stop() {
        this.serviceServer.stopServer();
    }
}
