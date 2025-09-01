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

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import org.apache.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import org.apache.geaflow.cluster.task.runner.ITaskRunner;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.common.utils.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskService<TASK, R extends ITaskRunner<TASK>> implements ITaskService<TASK> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskService.class);

    protected ExecutorService executorService;
    private R[] tasks;
    private String threadFormat;
    protected final Configuration configuration;

    public AbstractTaskService(Configuration configuration, String threadFormat) {
        this.threadFormat = threadFormat;
        this.configuration = configuration;
    }

    public void start() {
        this.tasks = buildTaskRunner();
        Preconditions.checkArgument(tasks != null && tasks.length != 0, "must specify at least one task");
        this.executorService = Executors.getExecutorService(getMaxMultiple(), tasks.length, threadFormat,
            ComponentUncaughtExceptionHandler.INSTANCE);
        for (int i = 0; i < tasks.length; i++) {
            executorService.execute(tasks[i]);
        }
    }

    /**
     * Provides the maximum thread multiplier value.
     *
     * @return the maximum thread multiplier
     */
    protected int getMaxMultiple() {
        return configuration.getInteger(ExecutionConfigKeys.EXECUTOR_MAX_MULTIPLE);
    }

    public void process(int workerId, TASK task) {
        tasks[workerId].add(task);
    }

    @Override
    public void interrupt(int workerId) {
        // TODO Interrupt specified worker running task.
        //      1. Try interrupt task runner.
        //      2. If failed or timeout, try shutdown and then rebuild executor service.
        //      3. If failed or timeout, report exception, may need exit process.
        tasks[workerId].interrupt();
    }

    @Override
    public void shutdown() {
        LOGGER.info("shutdown executor service {}", threadFormat);
        for (int i = 0; i < tasks.length; i++) {
            tasks[i].shutdown();
        }
        // try shutdown executor service
        ExecutorUtil.shutdown(executorService);
    }

    public R getRunner(int workerId) {
        return tasks[workerId];
    }

    protected abstract R[] buildTaskRunner();

}
