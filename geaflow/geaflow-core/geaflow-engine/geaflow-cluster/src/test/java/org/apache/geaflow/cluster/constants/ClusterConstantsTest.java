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

package org.apache.geaflow.cluster.constants;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterConstantsTest {

    @Test
    public void testDefaultValues() {
        // Test default values when no configuration is provided
        Assert.assertEquals(ClusterConstants.getMasterName(), "master-0");
        Assert.assertEquals(ClusterConstants.getDriverName(1), "driver-1");
        Assert.assertEquals(ClusterConstants.getContainerName(2), "container-2");
        
        Assert.assertEquals(ClusterConstants.MASTER_PREFIX, "master-");
        Assert.assertEquals(ClusterConstants.DRIVER_PREFIX, "driver-");
        Assert.assertEquals(ClusterConstants.CONTAINER_PREFIX, "container-");
        
        Assert.assertEquals(ClusterConstants.MASTER_LOG_SUFFIX, "master.log");
        Assert.assertEquals(ClusterConstants.DRIVER_LOG_SUFFIX, "driver.log");
        Assert.assertEquals(ClusterConstants.CONTAINER_LOG_SUFFIX, "container.log");
        
        Assert.assertEquals(ClusterConstants.DEFAULT_MASTER_ID, 0);
    }

    @Test
    public void testGetDriverPrefix_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_DRIVER_PREFIX.getKey(), "custom-driver-");
        
        String prefix = ClusterConstants.getDriverPrefix(config);
        Assert.assertEquals(prefix, "custom-driver-");
    }

    @Test
    public void testGetDriverPrefix_WithoutConfig() {
        Configuration config = new Configuration();
        String prefix = ClusterConstants.getDriverPrefix(config);
        Assert.assertEquals(prefix, ClusterConstants.DRIVER_PREFIX);
    }

    @Test
    public void testGetContainerPrefix_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_CONTAINER_PREFIX.getKey(), "custom-container-");
        
        String prefix = ClusterConstants.getContainerPrefix(config);
        Assert.assertEquals(prefix, "custom-container-");
    }

    @Test
    public void testGetContainerPrefix_WithoutConfig() {
        Configuration config = new Configuration();
        String prefix = ClusterConstants.getContainerPrefix(config);
        Assert.assertEquals(prefix, ClusterConstants.CONTAINER_PREFIX);
    }

    @Test
    public void testGetMasterLogSuffix_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_MASTER_LOG_SUFFIX.getKey(), "custom-master.log");
        
        String suffix = ClusterConstants.getMasterLogSuffix(config);
        Assert.assertEquals(suffix, "custom-master.log");
    }

    @Test
    public void testGetMasterLogSuffix_WithoutConfig() {
        Configuration config = new Configuration();
        String suffix = ClusterConstants.getMasterLogSuffix(config);
        Assert.assertEquals(suffix, ClusterConstants.MASTER_LOG_SUFFIX);
    }

    @Test
    public void testGetDriverLogSuffix_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_DRIVER_LOG_SUFFIX.getKey(), "custom-driver.log");
        
        String suffix = ClusterConstants.getDriverLogSuffix(config);
        Assert.assertEquals(suffix, "custom-driver.log");
    }

    @Test
    public void testGetContainerLogSuffix_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_CONTAINER_LOG_SUFFIX.getKey(), "custom-container.log");
        
        String suffix = ClusterConstants.getContainerLogSuffix(config);
        Assert.assertEquals(suffix, "custom-container.log");
    }

    @Test
    public void testGetDefaultMasterId_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_DEFAULT_MASTER_ID.getKey(), "1");
        
        int masterId = ClusterConstants.getDefaultMasterId(config);
        Assert.assertEquals(masterId, 1);
    }

    @Test
    public void testGetDefaultMasterId_WithoutConfig() {
        Configuration config = new Configuration();
        int masterId = ClusterConstants.getDefaultMasterId(config);
        Assert.assertEquals(masterId, ClusterConstants.DEFAULT_MASTER_ID);
    }

    @Test
    public void testGetMasterName_WithConfig() {
        Configuration config = new Configuration();
        // Master prefix is not configurable, only master id can be configured
        config.put(ExecutionConfigKeys.CLUSTER_DEFAULT_MASTER_ID.getKey(), "5");
        
        String masterName = ClusterConstants.getMasterName(config);
        Assert.assertEquals(masterName, "master-5");
    }

    @Test
    public void testGetMasterName_WithoutConfig() {
        Configuration config = new Configuration();
        String masterName = ClusterConstants.getMasterName(config);
        Assert.assertEquals(masterName, "master-0");
    }

    @Test
    public void testGetDriverName_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_DRIVER_PREFIX.getKey(), "custom-driver-");
        
        String driverName = ClusterConstants.getDriverName(config, 3);
        Assert.assertEquals(driverName, "custom-driver-3");
    }

    @Test
    public void testGetDriverName_WithoutConfig() {
        Configuration config = new Configuration();
        String driverName = ClusterConstants.getDriverName(config, 3);
        Assert.assertEquals(driverName, "driver-3");
    }

    @Test
    public void testGetContainerName_WithConfig() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.CLUSTER_CONTAINER_PREFIX.getKey(), "custom-container-");
        
        String containerName = ClusterConstants.getContainerName(config, 4);
        Assert.assertEquals(containerName, "custom-container-4");
    }

    @Test
    public void testGetContainerName_WithoutConfig() {
        Configuration config = new Configuration();
        String containerName = ClusterConstants.getContainerName(config, 4);
        Assert.assertEquals(containerName, "container-4");
    }

    @Test
    public void testBackwardCompatibility() {
        // Test that original methods still work without Configuration parameter
        Assert.assertEquals(ClusterConstants.getMasterName(), "master-0");
        Assert.assertEquals(ClusterConstants.getDriverName(1), "driver-1");
        Assert.assertEquals(ClusterConstants.getContainerName(2), "container-2");
    }
}

