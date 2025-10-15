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

import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentUncaughtExceptionHandler.class);

    public static final ComponentUncaughtExceptionHandler INSTANCE = new ComponentUncaughtExceptionHandler();

    @Override
    public void uncaughtException(Thread thread, Throwable cause) {
        LOGGER.error("FATAL exception in thread: {}", thread.getName(), cause);
        StatsCollectorFactory collectorFactory = StatsCollectorFactory.getInstance();
        if (collectorFactory != null) {
            collectorFactory.getExceptionCollector().reportException(ExceptionLevel.FATAL, cause);
        }
        ComponentExceptionSupervisor.getInstance()
            .add(ComponentExceptionSupervisor.ExceptionElement.of(thread, cause));
    }
}
