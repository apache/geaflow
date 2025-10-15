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

package org.apache.geaflow.cluster.task.runner;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.exception.GeaflowInterruptedException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskRunner<TASK> implements ITaskRunner<TASK> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskRunner.class);
    private static final int POOL_TIMEOUT = 100;

    private final LinkedBlockingQueue<TASK> taskQueue;
    protected volatile boolean running;

    public AbstractTaskRunner() {
        this.running = true;
        this.taskQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (running) {
            try {
                TASK task = taskQueue.poll(POOL_TIMEOUT, TimeUnit.MILLISECONDS);
                if (running && task != null) {
                    process(task);
                }
            } catch (InterruptedException e) {
                throw new GeaflowInterruptedException(e);
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
                throw new GeaflowRuntimeException(t);
            }
        }
    }

    @Override
    public void add(TASK task) {
        this.taskQueue.add(task);
    }

    protected abstract void process(TASK task);

    @Override
    public void interrupt() {
        // TODO interrupt running task.
    }

    @Override
    public void shutdown() {
        this.running = false;
    }
}
