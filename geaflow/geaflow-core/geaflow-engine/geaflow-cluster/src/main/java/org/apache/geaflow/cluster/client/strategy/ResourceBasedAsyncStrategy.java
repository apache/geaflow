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

package org.apache.geaflow.cluster.client.strategy;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource-based async mode strategy that considers driver and container resources.
 * This strategy automatically enables async mode when there are multiple drivers
 * or many containers to optimize resource utilization.
 */
public class ResourceBasedAsyncStrategy implements AsyncModeStrategy {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceBasedAsyncStrategy.class);
    private static final int DEFAULT_ASYNC_THRESHOLD_DRIVERS = 2;
    private static final int DEFAULT_ASYNC_THRESHOLD_CONTAINERS = 4;
    
    @Override
    public boolean shouldUseAsyncMode(Configuration config) {
        int driverNum = config.getInteger(ExecutionConfigKeys.DRIVER_NUM);
        int containerNum = config.getInteger(ExecutionConfigKeys.CONTAINER_NUM);
        
        // Get configurable thresholds, fall back to defaults if not specified
        int driverThreshold = config.getInteger(
            FrameworkConfigKeys.ASYNC_RESOURCE_DRIVER_THRESHOLD, DEFAULT_ASYNC_THRESHOLD_DRIVERS);
        int containerThreshold = config.getInteger(
            FrameworkConfigKeys.ASYNC_RESOURCE_CONTAINER_THRESHOLD, DEFAULT_ASYNC_THRESHOLD_CONTAINERS);
        
        // Use async mode if we have multiple drivers or many containers
        boolean shouldUseAsync = driverNum >= driverThreshold || containerNum >= containerThreshold;
        
        LOGGER.debug("Resource-based async strategy: drivers={}, containers={}, "
                    + "driverThreshold={}, containerThreshold={}, async={}", 
                    driverNum, containerNum, driverThreshold, containerThreshold, shouldUseAsync);
        
        return shouldUseAsync;
    }
    
    @Override
    public String getStrategyName() {
        return "resource-based";
    }
    
    @Override
    public void validateConfig(Configuration config) {
        int driverNum = config.getInteger(ExecutionConfigKeys.DRIVER_NUM);
        if (driverNum <= 0) {
            throw new IllegalArgumentException("Driver number must be positive, got: " + driverNum);
        }
        
        int containerNum = config.getInteger(ExecutionConfigKeys.CONTAINER_NUM);
        if (containerNum <= 0) {
            throw new IllegalArgumentException("Container number must be positive, got: " + containerNum);
        }
    }
}
