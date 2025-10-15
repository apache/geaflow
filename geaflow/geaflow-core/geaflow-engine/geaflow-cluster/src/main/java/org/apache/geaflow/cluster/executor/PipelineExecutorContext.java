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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.cluster.driver.DriverEventDispatcher;
import org.apache.geaflow.common.config.Configuration;

public class PipelineExecutorContext {

    private DriverEventDispatcher eventDispatcher;
    private Configuration envConfig;
    private String driverId;
    private int driverIndex;
    private AtomicInteger idGenerator;

    public PipelineExecutorContext(String driverId, int driverIndex, DriverEventDispatcher eventDispatcher,
                                   Configuration envConfig,
                                   AtomicInteger idGenerator) {
        this.eventDispatcher = eventDispatcher;
        this.envConfig = envConfig;
        this.driverId = driverId;
        this.driverIndex = driverIndex;
        this.idGenerator = idGenerator;
    }

    public DriverEventDispatcher getEventDispatcher() {
        return eventDispatcher;
    }

    public Configuration getEnvConfig() {
        return this.envConfig;
    }

    public String getDriverId() {
        return driverId;
    }

    public int getDriverIndex() {
        return driverIndex;
    }

    public AtomicInteger getIdGenerator() {
        return idGenerator;
    }
}


