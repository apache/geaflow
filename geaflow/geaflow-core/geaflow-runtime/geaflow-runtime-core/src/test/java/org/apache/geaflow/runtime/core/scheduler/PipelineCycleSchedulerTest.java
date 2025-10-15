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

package org.apache.geaflow.runtime.core.scheduler;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.ExecutionTaskType;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.runtime.core.protocol.ComposeEvent;
import org.apache.geaflow.runtime.core.protocol.LaunchSourceEvent;
import org.apache.geaflow.runtime.core.protocol.RollbackCycleEvent;
import org.apache.geaflow.runtime.core.scheduler.context.CheckpointSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PipelineCycleSchedulerTest extends BaseCycleSchedulerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineCycleSchedulerTest.class);

    private static CheckpointSchedulerContext mockPersistContext;
    private Configuration configuration;

    @BeforeMethod
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put(JOB_UNIQUE_ID.getKey(), "scheduler-fo-test" + System.currentTimeMillis());
        config.put(RUN_LOCAL_MODE.getKey(), "true");
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        config.put(CONTAINER_HEAP_SIZE_MB.getKey(), String.valueOf(1024));
        config.put(ExecutionConfigKeys.SHUFFLE_PREFETCH.getKey(), String.valueOf(false));
        configuration = new Configuration(config);
        ClusterMetaStore.init(0, "driver-0", configuration);
        ShuffleManager.init(configuration);
    }

    @AfterMethod
    public void cleanUp() {
        ClusterMetaStore.close();
        ScheduledWorkerManagerFactory.clear();
    }

    @Test(priority = 0)
    public void testSimplePipeline() {
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);
        StatsCollectorFactory.init(configuration);
        CheckpointSchedulerContext context = (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(configuration), null);
        mockPersistContext = context;
        scheduler.init(context);
        scheduler.execute();
        scheduler.close();

        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(6, events.size());

        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.INIT_CYCLE, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());
        Assert.assertEquals(1, ((LaunchSourceEvent) ((ComposeEvent) events.get(0)).getEventList().get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(1).getEventType());
        Assert.assertEquals(2, ((LaunchSourceEvent) events.get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(4).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(4)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(5).getEventType());

    }

    @Test(priority = 1)
    private void testPipelineAfterRestart() {

        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        // mock recover context from previous case.
        CheckpointSchedulerContext context = mockPersistContext;
        context.init(3);
        context.setRecovered(false);
        context.setRollback(true);

        scheduler.init(context);
        scheduler.execute();
        scheduler.close();

        context.setRollback(false);

        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.INIT_CYCLE, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(EventType.ROLLBACK, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());
        Assert.assertEquals(2, ((RollbackCycleEvent) ((ComposeEvent) events.get(0)).getEventList().get(1)).getIterationWindowId());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(0)).getEventList().get(2).getEventType());
        Assert.assertEquals(3, ((LaunchSourceEvent) ((ComposeEvent) events.get(0)).getEventList().get(2)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(1).getEventType());
        Assert.assertEquals(4, ((LaunchSourceEvent) events.get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(2).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(2)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(3).getEventType());
    }

    @Test(priority = 1)
    private void testPipelineAfterRecover() {

        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        // mock recover context from previous case.
        CheckpointSchedulerContext context = mockPersistContext;
        context.init(4);
        context.setRecovered(true);

        scheduler.init(context);
        scheduler.execute();
        scheduler.close();


        List<IEvent> events = processor.getProcessed();
        LOGGER.info("processed events {}", events.size());
        for (IEvent event : events) {
            LOGGER.info("{}", event);
        }
        Assert.assertEquals(3, events.size());
        Assert.assertEquals(EventType.COMPOSE, events.get(0).getEventType());
        Assert.assertEquals(EventType.ROLLBACK, ((ComposeEvent) events.get(0)).getEventList().get(0).getEventType());
        Assert.assertEquals(3, ((RollbackCycleEvent) ((ComposeEvent) events.get(0)).getEventList().get(0)).getIterationWindowId());
        Assert.assertEquals(EventType.LAUNCH_SOURCE, ((ComposeEvent) events.get(0)).getEventList().get(1).getEventType());
        Assert.assertEquals(4, ((LaunchSourceEvent) ((ComposeEvent) events.get(0)).getEventList().get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.LAUNCH_SOURCE, events.get(1).getEventType());
        Assert.assertEquals(5, ((LaunchSourceEvent) events.get(1)).getIterationWindowId());

        Assert.assertEquals(EventType.CLEAN_CYCLE, events.get(2).getEventType());
    }

    private ExecutionNodeCycle buildMockCycle(Configuration configuration) {

        long finishIterationId = 5;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(1);
        vertexGroup.getVertexMap().put(0, vertex);
        vertexGroup.putVertexId2InEdgeIds(0, new ArrayList<>());
        vertexGroup.putVertexId2OutEdgeIds(0, new ArrayList<>());

        List<ExecutionTask> headTasks = new ArrayList<>();
        List<ExecutionTask> tailTasks = new ArrayList<>();
        for (int i = 0; i < vertex.getParallelism(); i++) {
            ExecutionTask task = new ExecutionTask(i, i, vertex.getParallelism(), vertex.getParallelism(), vertex.getParallelism(), vertex.getVertexId());
            task.setWorkerInfo(new WorkerInfo("host0", 0, 0, 1, -1, 1, "container0"));
            task.setExecutionTaskType(ExecutionTaskType.head);
            tailTasks.add(task);
            headTasks.add(task);
        }

        ExecutionNodeCycle cycle = new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup,
            configuration, "driver_id", 0);
        cycle.setCycleHeads(headTasks);
        cycle.setCycleTails(tailTasks);
        cycle.setTasks(headTasks);
        return cycle;
    }

}
