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

package org.apache.geaflow.cluster.worker;

import org.apache.geaflow.cluster.protocol.ICommand;
import org.apache.geaflow.cluster.protocol.IComposeEvent;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.task.runner.AbstractTaskRunner;
import org.apache.geaflow.cluster.task.service.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher extends AbstractTaskRunner<ICommand> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);

    private TaskService taskService;

    public Dispatcher(TaskService taskService) {
        super();
        this.taskService = taskService;
    }

    @Override
    protected void process(ICommand command) {
        switch (command.getEventType()) {
            case COMPOSE:
                for (IEvent event : ((IComposeEvent) command).getEventList()) {
                    process((ICommand) event);
                }
                break;
            case INTERRUPT_TASK:
                LOGGER.info("{} interrupt current running task", command.getWorkerId());
                this.taskService.interrupt(command.getWorkerId());
                break;
            default:
                this.taskService.process(command.getWorkerId(), command);
                break;

        }
    }

}
