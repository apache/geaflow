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

package org.apache.geaflow.console.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;
import org.apache.geaflow.console.core.model.job.config.GeaflowArgsClass;
import org.apache.geaflow.console.core.model.job.config.HaMetaArgsClass;
import org.apache.geaflow.console.core.model.job.config.JobArgsClass;
import org.apache.geaflow.console.core.model.job.config.JobConfigClass;
import org.apache.geaflow.console.core.model.job.config.K8SClusterArgsClass;
import org.apache.geaflow.console.core.model.job.config.K8sClientArgsClass;
import org.apache.geaflow.console.core.model.job.config.K8sClientStopArgsClass;
import org.apache.geaflow.console.core.model.job.config.MetricArgsClass;
import org.apache.geaflow.console.core.model.job.config.PersistentArgsClass;
import org.apache.geaflow.console.core.model.job.config.RuntimeMetaArgsClass;
import org.apache.geaflow.console.core.model.job.config.StateArgsClass;
import org.apache.geaflow.console.core.model.job.config.SystemArgsClass;
import org.apache.geaflow.console.core.model.plugin.config.DfsPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.plugin.config.InfluxdbPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.K8sPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.PluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.RedisPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class GeaflowConfigClassTest {

    @Test()
    public void testGeaflowArgs() {
        Assert.assertEquals(buildGeaflowClientArgs().build().size(), 35);
        Assert.assertEquals(buildGeaflowClientStopArgs().build().size(), 31);
        Assert.assertEquals(buildGeaflowArgs().build().size(), 3);
        Assert.assertEquals(buildSystemArgs().build().size(), 10);
        Assert.assertEquals(buildClusterArgs().build().size(), 30);
        Assert.assertEquals(buildJobArgs().build().size(), 2);
        Assert.assertEquals(buildStateArgs().build().size(), 19);
        Assert.assertEquals(buildMetricArgs().build().size(), 7);
    }

    private static K8sClientArgsClass buildGeaflowClientArgs() {
        return new K8sClientArgsClass(buildGeaflowArgs(), GeaflowTask.CODE_TASK_MAIN_CLASS);
    }

    private static K8sClientStopArgsClass buildGeaflowClientStopArgs() {
        return new K8sClientStopArgsClass("geaflow123456-123456", buildClusterArgs());
    }

    private static GeaflowArgsClass buildGeaflowArgs() {
        GeaflowArgsClass config = new GeaflowArgsClass();
        config.setSystemArgs(buildSystemArgs());
        config.setClusterArgs(buildClusterArgs());
        config.setJobArgs(buildJobArgs());
        return config;
    }

    private static SystemArgsClass buildSystemArgs() {
        SystemArgsClass config = new SystemArgsClass();
        config.setTaskId("123455");
        config.setRuntimeTaskId("geaflow123456-123456");
        config.setRuntimeTaskName("geaflow123456");
        config.setGateway("http://127.0.0.1:8888");
        config.setTaskToken("qwertyuiopasdfghjklzxcvbnm");
        config.setStartupNotifyUrl("http://127.0.0.1:8888/api/tasks/123455/operations");
        config.setInstanceName("test-instance");
        config.setCatalogType("console");
        config.setStateArgs(buildStateArgs());
        config.setMetricArgs(buildMetricArgs());
        return config;
    }

    private static StateArgsClass buildStateArgs() {
        StateArgsClass stateArgs = new StateArgsClass();
        stateArgs.setRuntimeMetaArgs(new RuntimeMetaArgsClass(
            new GeaflowPluginConfig(GeaflowPluginCategory.RUNTIME_META, buildJdbcPluginConfig())));
        stateArgs.setHaMetaArgs(
            new HaMetaArgsClass(new GeaflowPluginConfig(GeaflowPluginCategory.HA_META, buildRedisPluginConfig())));
        stateArgs.setPersistentArgs(
            new PersistentArgsClass(new GeaflowPluginConfig(GeaflowPluginCategory.DATA, buildDfsPluginConfig())));
        return stateArgs;
    }

    private static MetricArgsClass buildMetricArgs() {
        return new MetricArgsClass(new GeaflowPluginConfig(GeaflowPluginCategory.METRIC, buildInfluxPluginConfig()));
    }

    private static K8SClusterArgsClass buildClusterArgs() {
        K8SClusterArgsClass config = new K8SClusterArgsClass();
        config.setTaskClusterConfig(buildClusterConfig());
        config.setClusterConfig(buildK8sPluginConfig());
        config.setEngineJarUrls("http://127.0.0.1/geaflow/files/versions/0.1/geaflow.jar");
        config.setTaskJarUrls("http://127.0.0.1/geaflow/files/users/123456/udf.jar");
        return config;
    }

    private static JobArgsClass buildJobArgs() {
        JobArgsClass config = new JobArgsClass();
        config.setSystemStateType(GeaflowPluginType.ROCKSDB);
        config.setJobConfig(buildJobConfig());
        return config;
    }

    private static K8sPluginConfigClass buildK8sPluginConfig() {
        K8sPluginConfigClass config = new K8sPluginConfigClass();
        config.setMasterUrl("https://0.0.0.0:6443");
        config.setImageUrl("tugraph/geaflow:0.1");
        config.setServiceAccount("geaflow");
        config.setServiceType("NODE_PORT");
        config.setNamespace("default");
        config.setCertData("xxx");
        config.setCertKey("yyy");
        config.setCaData("zzz");
        config.setRetryTimes(100);
        config.setClusterName("aaa");
        config.setPodUserLabels("bbb");
        config.setServiceSuffix("ccc");
        config.setStorageLimit("50Gi");
        return config;
    }

    private static JdbcPluginConfigClass buildJdbcPluginConfig() {
        JdbcPluginConfigClass config = new JdbcPluginConfigClass();
        config.setDriverClass("com.mysql.jdbc.Driver");
        config.setUrl("jdbc:mysql://127..0.0.1:3306/geaflow");
        config.setUsername("geaflow");
        config.setPassword("geaflow");
        config.setRetryTimes(3);
        config.setConnectionPoolSize(10);
        config.setConfigJson("{}");
        return config;
    }

    private static RedisPluginConfigClass buildRedisPluginConfig() {
        RedisPluginConfigClass config = new RedisPluginConfigClass();
        config.setHost("127.0.0.1");
        config.setPort(6379);
        config.setRetryTimes(10);
        return config;
    }

    private static PluginConfigClass buildInfluxPluginConfig() {
        InfluxdbPluginConfigClass config = new InfluxdbPluginConfigClass();
        config.setUrl("http://127.0.0.1:8086");
        config.setToken("qwertyuiopkjhgfdxcvb");
        config.setOrg("geaflow");
        config.setBucket("geaflow");
        config.setConnectTimeout(30000);
        config.setWriteTimeout(30000);
        return config;
    }

    private static DfsPluginConfigClass buildDfsPluginConfig() {
        DfsPluginConfigClass config = new DfsPluginConfigClass();
        config.setDefaultFs("hdfs://127.0.0.1");
        config.setRoot("/geaflow/chk");
        config.setThreadSize(16);
        config.setUsername("geaflow");
        config.getExtendConfig().put("fs.custom", "custom");
        return config;
    }

    private static ClusterConfigClass buildClusterConfig() {
        ClusterConfigClass config = new ClusterConfigClass();
        config.setContainers(1);
        config.setContainerMemory(4096);
        config.setContainerCores(1.0);
        config.setContainerWorkers(1);
        config.setContainerJvmOptions("-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m");
        config.setEnableFo(true);
        config.setMasterMemory(4096);
        config.setDriverMemory(4096);
        config.setClientMemory(1024);
        config.setMasterCores(1.0);
        config.setDriverCores(1.0);
        config.setClientCores(1.0);
        config.setMasterJvmOptions("-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m");
        config.setDriverJvmOptions("-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m");
        config.setClientJvmOptions("-Xmx1024m,-Xms1024m,-Xmn256m,-Xss256k,-XX:MaxDirectMemorySize=512m");
        return config;
    }

    private static JobConfigClass buildJobConfig() {
        JobConfigClass config = new JobConfigClass();
        config.getExtendConfig().put("job.custom", "custom");
        return config;
    }
}
