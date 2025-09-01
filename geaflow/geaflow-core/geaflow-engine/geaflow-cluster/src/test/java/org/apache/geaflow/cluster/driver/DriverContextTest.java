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

package org.apache.geaflow.cluster.driver;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;
import static org.mockito.Matchers.any;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.common.ExecutionIdGenerator;
import org.apache.geaflow.cluster.failover.FailoverStrategyType;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DriverContextTest {

    private Configuration configuration = new Configuration();

    @BeforeMethod
    public void before() {
        String path = "/tmp/" + DriverContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), DriverContextTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
        ExecutionIdGenerator.init(0);
    }

    @AfterMethod
    public void after() {
        String path = "/tmp/" + DriverContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        ClusterMetaStore.close();
    }

    @Test
    public void testRecoverContext() {

        int driverId = 1;
        ClusterMetaStore.init(driverId, "driver-0", configuration);
        DriverContext driverContext = new DriverContext(driverId, 0, configuration);

        Environment environment = Mockito.mock(Environment.class);
        Mockito.doNothing().when(environment).addPipeline(any());
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.getPipelineTaskList().add(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {

            }
        });
        driverContext.addPipeline(pipeline);
        List<Long> pipelineTaskIds = driverContext.getPipelineTaskIds();
        driverContext.addFinishedPipelineTask(0);
        driverContext.addFinishedPipelineTask(1);
        driverContext.checkpoint(new DriverContext.PipelineCheckpointFunction());
        driverContext.checkpoint(new DriverContext.PipelineTaskCheckpointFunction());

        DriverContext newContext = new DriverContext(driverId, 0, configuration);
        newContext.load();

        Assert.assertNotNull(pipeline);
        Assert.assertEquals(2, newContext.getFinishedPipelineTasks().size());
        Assert.assertEquals(0, newContext.getFinishedPipelineTasks().get(0).intValue());
        Assert.assertEquals(1, newContext.getFinishedPipelineTasks().get(1).intValue());
        Assert.assertEquals(pipelineTaskIds.get(0), newContext.getPipelineTaskIds().get(0));

        // ---- mock restart job ----
        // cluster id is changed, re-init cluster metastore.
        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test2");
        ClusterMetaStore.init(driverId, "driver-0", configuration);
        // rebuild, context reliable event list is empty, and metastore is cleaned.
        DriverContext restarted = new DriverContext(driverId, 0, configuration);
        restarted.load();
        Assert.assertNull(restarted.getPipeline());
        Assert.assertTrue(restarted.getFinishedPipelineTasks().isEmpty());
        Assert.assertTrue(restarted.getPipelineTaskIds().isEmpty());
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class,
        expectedExceptionsMessageRegExp = "not support component_fo for executing pipeline tasks")
    public void testPipelineAndCheckFoStrategy() {

        int driverId = 1;
        Configuration recoverConfig = new Configuration(configuration.getConfigMap());
        recoverConfig.put(FO_STRATEGY, FailoverStrategyType.component_fo.name());
        DriverContext driverContext = new DriverContext(driverId, 0, recoverConfig);

        Environment environment = Mockito.mock(Environment.class);
        Mockito.doNothing().when(environment).addPipeline(any());
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.getPipelineTaskList().add(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {

            }
        });
        driverContext.addPipeline(pipeline);
    }
}
