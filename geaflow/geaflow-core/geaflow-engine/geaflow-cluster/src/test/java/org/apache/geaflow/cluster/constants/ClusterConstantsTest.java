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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterConstantsTest {

    @Test
    public void testDefaultValues() {
        // Test default values
        Assert.assertEquals(ClusterConstants.getMasterName(), "master-0");
        Assert.assertEquals(ClusterConstants.getDriverName(1), "driver-1");
        Assert.assertEquals(ClusterConstants.getContainerName(2), "container-2");
        
        Assert.assertEquals(ClusterConstants.MASTER_LOG_SUFFIX, "master.log");
        Assert.assertEquals(ClusterConstants.DRIVER_LOG_SUFFIX, "driver.log");
        Assert.assertEquals(ClusterConstants.CONTAINER_LOG_SUFFIX, "container.log");
        
        Assert.assertEquals(ClusterConstants.DEFAULT_MASTER_ID, 0);
    }

    @Test
    public void testGetMasterName() {
        String masterName = ClusterConstants.getMasterName();
        Assert.assertEquals(masterName, "master-0");
    }

    @Test
    public void testGetDriverName() {
        Assert.assertEquals(ClusterConstants.getDriverName(0), "driver-0");
        Assert.assertEquals(ClusterConstants.getDriverName(1), "driver-1");
        Assert.assertEquals(ClusterConstants.getDriverName(10), "driver-10");
    }

    @Test
    public void testGetContainerName() {
        Assert.assertEquals(ClusterConstants.getContainerName(0), "container-0");
        Assert.assertEquals(ClusterConstants.getContainerName(1), "container-1");
        Assert.assertEquals(ClusterConstants.getContainerName(100), "container-100");
    }

    @Test
    public void testConstants() {
        // Test all constants are properly defined
        Assert.assertNotNull(ClusterConstants.MASTER_LOG_SUFFIX);
        Assert.assertNotNull(ClusterConstants.DRIVER_LOG_SUFFIX);
        Assert.assertNotNull(ClusterConstants.CONTAINER_LOG_SUFFIX);
        Assert.assertNotNull(ClusterConstants.CLUSTER_TYPE);
        Assert.assertNotNull(ClusterConstants.LOCAL_CLUSTER);
        Assert.assertNotNull(ClusterConstants.MASTER_ID);
        Assert.assertNotNull(ClusterConstants.CONTAINER_ID);
        Assert.assertNotNull(ClusterConstants.CONTAINER_INDEX);
    }
}

