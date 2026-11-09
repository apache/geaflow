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

/**
 * Strategy interface for determining async mode.
 * This interface provides a pluggable way to determine whether async mode should be used
 * based on different factors such as configuration, resources, or workload.
 */
public interface AsyncModeStrategy {
    
    /**
     * Determine whether to use async mode based on configuration.
     * 
     * @param config the configuration object containing relevant settings
     * @return true if async mode should be used, false otherwise
     */
    boolean shouldUseAsyncMode(Configuration config);
    
    /**
     * Get strategy name for identification and registration.
     * 
     * @return the unique name of this strategy
     */
    String getStrategyName();
    
    /**
     * Validate configuration for this strategy.
     * This method can be overridden to provide configuration validation.
     * 
     * @param config the configuration to validate
     * @throws IllegalArgumentException if the configuration is invalid for this strategy
     */
    default void validateConfig(Configuration config) {
        // Default implementation does nothing
    }
}
