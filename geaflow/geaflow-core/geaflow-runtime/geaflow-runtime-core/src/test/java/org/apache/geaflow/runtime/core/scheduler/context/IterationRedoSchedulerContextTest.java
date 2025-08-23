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

import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IterationRedoSchedulerContextTest extends BaseCycleSchedulerContextTest {

    @Test
    public void testIterationFinishId() {
        long finishIterationId = 100;
        ExecutionNodeCycle cycle = buildMockCycle(false, finishIterationId);
        cycle.getVertexGroup().getCycleGroupMeta().setIterationCount(finishIterationId);
        CheckpointSchedulerContext parentContext = new CheckpointSchedulerContext(cycle, null);
        parentContext.init(50);

        ExecutionNodeCycle iterationCycle = buildMockCycle(false, 5);
        IterationRedoSchedulerContext iterationContext = new IterationRedoSchedulerContext(iterationCycle, parentContext);
        iterationContext.init(20);

        Assert.assertEquals(iterationContext.getCurrentIterationId(), 1);
        Assert.assertEquals(iterationContext.getFinishIterationId(), 5);
    }

    protected ExecutionNodeCycle buildMockCycle(boolean isIterative, long iterationCount) {
        ClusterMetaStore.init(0, "driver-0", configuration);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(iterationCount);
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
