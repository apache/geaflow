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
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hybrid async mode strategy that combines multiple factors.
 * This strategy first checks explicit configuration, then falls back to resource-based decision.
 */
public class HybridAsyncStrategy implements AsyncModeStrategy {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HybridAsyncStrategy.class);
    
    private final ConfigBasedAsyncStrategy configStrategy = new ConfigBasedAsyncStrategy();
    private final ResourceBasedAsyncStrategy resourceStrategy = new ResourceBasedAsyncStrategy();
    
    @Override
    public boolean shouldUseAsyncMode(Configuration config) {
        // First check explicit configuration
        if (config.contains(FrameworkConfigKeys.SERVICE_SHARE_ENABLE)) {
            boolean configResult = configStrategy.shouldUseAsyncMode(config);
            LOGGER.debug("Hybrid strategy using explicit config: {}", configResult);
            return configResult;
        }
        
        // Fall back to resource-based decision
        boolean resourceResult = resourceStrategy.shouldUseAsyncMode(config);
        LOGGER.debug("Hybrid strategy using resource-based decision: {}", resourceResult);
        return resourceResult;
    }
    
    @Override
    public String getStrategyName() {
        return "hybrid";
    }
    
    @Override
    public void validateConfig(Configuration config) {
        resourceStrategy.validateConfig(config);
    }
}
