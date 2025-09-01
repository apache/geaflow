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

package org.apache.geaflow.cluster.config;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_JVM_OPTION;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_MEMORY_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_JVM_OPTIONS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_MEMORY_MB;

import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterConfigTest {

    @Test
    public void testMasterConfig() {
        ClusterConfig clusterConfig = ClusterConfig.build(new Configuration());
        ClusterJvmOptions masterOptions = clusterConfig.getMasterJvmOptions();

        Assert.assertEquals(masterOptions.getJvmOptions().size(), 4);
        Assert.assertEquals(masterOptions.getExtraOptions().size(), 1);
    }

    @Test
    public void test() {
        Configuration config = new Configuration();
        config.put(CONTAINER_MEMORY_MB.getKey(), "256");
        config.put(CONTAINER_JVM_OPTION.getKey(), "-Xmx200m,-Xms200m,-Xmn128m");
        config.put(MASTER_MEMORY_MB.getKey(), "256");
        config.put(MASTER_JVM_OPTIONS.getKey(), "-Xmx200m,-Xms200m,-Xmn128m");
        ClusterConfig clusterConfig = ClusterConfig.build(config);
        Assert.assertNotNull(clusterConfig);
        Assert.assertNotNull(clusterConfig.getMasterJvmOptions());
        Assert.assertNotNull(clusterConfig.getDriverJvmOptions());
        Assert.assertNotNull(clusterConfig.getContainerJvmOptions());
    }

}
