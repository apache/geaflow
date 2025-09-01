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

package org.apache.geaflow.cluster.exception;

import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;

import org.apache.geaflow.cluster.task.runner.AbstractTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentExceptionSupervisor extends AbstractTaskRunner<ComponentExceptionSupervisor.ExceptionElement> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentExceptionSupervisor.class);

    private static ComponentExceptionSupervisor INSTANCE;

    @Override
    protected void process(ExceptionElement exceptionElement) {
        // Send exception to master.
        ExceptionClient exceptionClient = ExceptionClient.getInstance();
        if (exceptionClient != null) {
            exceptionClient.sendException(exceptionElement.cause);
        }
        // Exit current process if supervisor is running.
        if (running) {
            LOGGER.error(String.format("%s occur fatal exception, exit process now",
                exceptionElement.thread), exceptionElement.cause);
            System.exit(EXIT_CODE);
        } else {
            LOGGER.info("{} ignore exception because supervisor is shutdown", exceptionElement.thread);
        }
    }

    public static synchronized ComponentExceptionSupervisor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ComponentExceptionSupervisor();
        }
        return INSTANCE;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.INSTANCE = null;
    }

    public static class ExceptionElement {

        private Thread thread;
        private Throwable cause;

        public ExceptionElement(Thread thread, Throwable cause) {
            this.thread = thread;
            this.cause = cause;
        }

        public static ExceptionElement of(Thread thread, Throwable cause) {
            return new ExceptionElement(thread, cause);
        }
    }
}
