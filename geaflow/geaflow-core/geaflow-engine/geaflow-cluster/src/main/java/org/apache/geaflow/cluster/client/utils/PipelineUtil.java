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

package org.apache.geaflow.cluster.client.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.client.strategy.AsyncModeStrategy;
import org.apache.geaflow.cluster.client.strategy.ConfigBasedAsyncStrategy;
import org.apache.geaflow.cluster.client.strategy.HybridAsyncStrategy;
import org.apache.geaflow.cluster.client.strategy.ResourceBasedAsyncStrategy;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for pipeline operations including async mode determination.
 * This class provides a pluggable strategy-based approach for determining
 * whether to use async mode based on different factors.
 */
public class PipelineUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineUtil.class);
    private static final Map<String, AsyncModeStrategy> strategies = new HashMap<>();
    private static final String DEFAULT_STRATEGY = "config-based";
    
    static {
        // Register available strategies
        registerStrategy(new ConfigBasedAsyncStrategy());
        registerStrategy(new ResourceBasedAsyncStrategy());
        registerStrategy(new HybridAsyncStrategy());
    }
    
    /**
     * Determine if async mode should be used based on configuration and strategy.
     * 
     * @param configuration the configuration object
     * @return true if async mode should be used, false otherwise
     */
    public static boolean isAsync(Configuration configuration) {
        try {
            String strategyName = configuration.getString(
                FrameworkConfigKeys.ASYNC_MODE_STRATEGY, DEFAULT_STRATEGY);
            
            AsyncModeStrategy strategy = strategies.get(strategyName);
            if (strategy == null) {
                LOGGER.warn("Unknown async strategy '{}', falling back to default", strategyName);
                strategy = strategies.get(DEFAULT_STRATEGY);
            }
            
            // Validate configuration before using strategy
            strategy.validateConfig(configuration);
            
            boolean result = strategy.shouldUseAsyncMode(configuration);
            LOGGER.debug("Async mode determination: strategy={}, result={}", 
                        strategy.getStrategyName(), result);
            
            return result;
        } catch (Exception e) {
            LOGGER.error("Error determining async mode, falling back to config-based strategy", e);
            // Fallback to original logic for safety
            return configuration.getBoolean(FrameworkConfigKeys.SERVICE_SHARE_ENABLE);
        }
    }
    
    /**
     * Register a new async mode strategy.
     * 
     * @param strategy the strategy to register
     */
    public static void registerStrategy(AsyncModeStrategy strategy) {
        strategies.put(strategy.getStrategyName(), strategy);
        LOGGER.debug("Registered async strategy: {}", strategy.getStrategyName());
    }
    
    /**
     * Get available strategy names.
     * 
     * @return array of available strategy names
     */
    public static String[] getAvailableStrategies() {
        return strategies.keySet().toArray(new String[0]);
    }
}
