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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for PipelineUtil async mode determination functionality.
 */
public class PipelineUtilTest {

    @Test
    public void testConfigBasedStrategy() {
        Configuration config = new Configuration();
        
        // Test with SERVICE_SHARE_ENABLE = true
        config.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE.getKey(), "true");
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "config-based");
        Assert.assertTrue(PipelineUtil.isAsync(config));
        
        // Test with SERVICE_SHARE_ENABLE = false
        config.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE.getKey(), "false");
        Assert.assertFalse(PipelineUtil.isAsync(config));
    }

    @Test
    public void testResourceBasedStrategy() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "resource-based");
        
        // Test with single driver and container (should not use async)
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "1");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "1");
        Assert.assertFalse(PipelineUtil.isAsync(config));
        
        // Test with multiple drivers (should use async)
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "2");
        Assert.assertTrue(PipelineUtil.isAsync(config));
        
        // Test with many containers (should use async)
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "1");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "4");
        Assert.assertTrue(PipelineUtil.isAsync(config));
    }

    @Test
    public void testHybridStrategy() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "hybrid");
        
        // Test with explicit SERVICE_SHARE_ENABLE (should use config-based decision)
        config.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE.getKey(), "true");
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "1");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "1");
        Assert.assertTrue(PipelineUtil.isAsync(config));
        
        // Test without explicit SERVICE_SHARE_ENABLE (should use resource-based decision)
        config.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE.getKey(), null);
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "2");
        Assert.assertTrue(PipelineUtil.isAsync(config));
    }

    @Test
    public void testUnknownStrategy() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "unknown-strategy");
        config.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE.getKey(), "true");
        
        // Should fall back to config-based strategy
        Assert.assertTrue(PipelineUtil.isAsync(config));
    }

    @Test
    public void testAvailableStrategies() {
        String[] strategies = PipelineUtil.getAvailableStrategies();
        Assert.assertNotNull(strategies);
        Assert.assertTrue(strategies.length >= 3);
        
        // Check that all expected strategies are available
        boolean hasConfigBased = false;
        boolean hasResourceBased = false;
        boolean hasHybrid = false;
        
        for (String strategy : strategies) {
            if ("config-based".equals(strategy)) {
                hasConfigBased = true;
            } else if ("resource-based".equals(strategy)) {
                hasResourceBased = true;
            } else if ("hybrid".equals(strategy)) {
                hasHybrid = true;
            }
        }
        
        Assert.assertTrue(hasConfigBased, "config-based strategy should be available");
        Assert.assertTrue(hasResourceBased, "resource-based strategy should be available");
        Assert.assertTrue(hasHybrid, "hybrid strategy should be available");
    }

    @Test
    public void testCustomThresholds() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "resource-based");
        
        // Test with custom driver threshold
        config.put(FrameworkConfigKeys.ASYNC_RESOURCE_DRIVER_THRESHOLD.getKey(), "3");
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "2");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "1");
        Assert.assertFalse(PipelineUtil.isAsync(config));
        
        // Test with custom container threshold
        config.put(FrameworkConfigKeys.ASYNC_RESOURCE_CONTAINER_THRESHOLD.getKey(), "6");
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "1");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "4");
        Assert.assertFalse(PipelineUtil.isAsync(config));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidDriverNum() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "resource-based");
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "0");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "1");
        
        PipelineUtil.isAsync(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidContainerNum() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.ASYNC_MODE_STRATEGY.getKey(), "resource-based");
        config.put(ExecutionConfigKeys.DRIVER_NUM.getKey(), "1");
        config.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), "-1");
        
        PipelineUtil.isAsync(config);
    }
}
