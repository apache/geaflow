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

package org.apache.geaflow.cluster.task.service;

import org.apache.geaflow.cluster.collector.EmitterService;
import org.apache.geaflow.cluster.fetcher.FetcherService;
import org.apache.geaflow.cluster.protocol.ICommand;
import org.apache.geaflow.cluster.task.runner.TaskRunner;
import org.apache.geaflow.cluster.task.runner.TaskRunnerContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskService extends AbstractTaskService<ICommand, TaskRunner> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskService.class);

    private static final String WORKER_FORMAT = "geaflow-worker-%d";

    private int containerId;
    private int taskNum;
    private MetricGroup metricGroup;
    private FetcherService fetcherService;
    private EmitterService emitterService;

    public TaskService(int containerId, int taskNum, Configuration configuration,
                       MetricGroup metricGroup, FetcherService fetcherService, EmitterService emitterService) {
        super(configuration, WORKER_FORMAT);
        this.containerId = containerId;
        this.taskNum = taskNum;
        this.metricGroup = metricGroup;
        this.fetcherService = fetcherService;
        this.emitterService = emitterService;
    }

    @Override
    protected TaskRunner[] buildTaskRunner() {
        TaskRunner[] taskRunners = new TaskRunner[taskNum];
        for (int i = 0; i < taskNum; i++) {
            TaskRunner taskRunner = buildTask(containerId, i, configuration,
                metricGroup, fetcherService, emitterService);
            taskRunners[i] = taskRunner;
        }
        return taskRunners;
    }

    private TaskRunner buildTask(int containerId, int taskIndex,
                                 Configuration configuration, MetricGroup metricGroup,
                                 FetcherService fetcherService, EmitterService emitterService) {
        TaskRunner taskRunner = new TaskRunner();
        TaskRunnerContext taskRunnerContext = new TaskRunnerContext(containerId, taskIndex,
            configuration, metricGroup, fetcherService, emitterService);
        taskRunner.init(taskRunnerContext);
        return taskRunner;
    }
}
