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

package org.apache.geaflow.runtime.core.scheduler.context;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.state.StoreType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractCycleSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testFinishIterationIdFromRecover() {
        long finishIterationId = 100;
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext context = new CheckpointSchedulerContext(cycle, null);
        context.init();

        long checkpointId = 20L;
        context.checkpoint(checkpointId);
        CheckpointSchedulerContext newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), null);

        Assert.assertEquals(checkpointId + 1, newContext.getCurrentIterationId());
        Assert.assertEquals(finishIterationId, newContext.getFinishIterationId());
    }

    @Test
    public void testInitContextAfterRecover() {
        long finishIterationId = 100;
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext context = new CheckpointSchedulerContext(cycle, null);
        context.init();

        long checkpointId = 20L;
        context.checkpoint(checkpointId);
        CheckpointSchedulerContext newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), null);

        long currentIterationId = checkpointId + 1;
        context.init(currentIterationId);

        Assert.assertEquals(currentIterationId, newContext.getCurrentIterationId());
        Assert.assertEquals(currentIterationId, newContext.getInitialIterationId());
        Assert.assertEquals(finishIterationId, newContext.getFinishIterationId());

        Assert.assertNotNull(context.getResultManager());
        Assert.assertNotNull(context.getSchedulerWorkerManager());
    }


    @Test
    public void testInitContextAfterRestart() {
        long finishIterationId = 100;
        ExecutionNodeCycle cycle = buildMockCycle(false);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext context = new CheckpointSchedulerContext(cycle, null);
        context.init();

        // do checkpoint
        long checkpointId = 20L;
        context.checkpoint(checkpointId);

        // clean checkpoint cycle.
        ClusterMetaStore.getInstance(0, "driver-0", new Configuration()).clean();
        CheckpointSchedulerContext newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(),
                () -> CycleSchedulerContextFactory.create(cycle, null));

        long currentIterationId = checkpointId + 1;
        context.init(currentIterationId);

        Assert.assertEquals(currentIterationId, newContext.getCurrentIterationId());
        Assert.assertEquals(currentIterationId, newContext.getInitialIterationId());
        Assert.assertEquals(finishIterationId, newContext.getFinishIterationId());

        Assert.assertNotNull(context.getResultManager());
        Assert.assertNotNull(context.getSchedulerWorkerManager());
    }

    @Test
    public void testCheckpointDuration() {

        ExecutionNodeCycle cycle = buildMockCycle(false);
        CheckpointSchedulerContext context = new CheckpointSchedulerContext(cycle, null);
        context.init();

        // not do checkpoint at 17
        long checkpointId = 17L;
        context.checkpoint(checkpointId);
        Assert.assertNull(ClusterMetaStore.getInstance().getWindowId(context.getCycle().getPipelineTaskId()));
        CheckpointSchedulerContext newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), null);
        Assert.assertNotNull(newContext);
        Assert.assertEquals(1, newContext.getCurrentIterationId());

        checkpointId = 20L;
        context.checkpoint(checkpointId);
        newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), null);
        Assert.assertEquals(checkpointId + 1, newContext.getCurrentIterationId());


        // loaded is still previous checkpointId
        long newCheckpointId = 23L;
        context.checkpoint(newCheckpointId);
        newContext =
            (CheckpointSchedulerContext) CheckpointSchedulerContext.build(context.getCycle().getPipelineTaskId(), null);
        Assert.assertEquals(checkpointId + 1, newContext.getCurrentIterationId());
    }

    private ExecutionNodeCycle buildMockCycle(boolean isIterative) {
        Configuration configuration = new Configuration();
        configuration.put(JOB_UNIQUE_ID, "test-scheduler-context");
        configuration.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        ClusterMetaStore.init(0, "driver-0", configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        if (isIterative) {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
        } else {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        }
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup, configuration, "driver_id", 0);
    }
}
