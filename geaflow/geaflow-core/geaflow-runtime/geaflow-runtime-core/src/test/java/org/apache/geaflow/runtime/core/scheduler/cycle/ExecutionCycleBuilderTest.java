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

package org.apache.geaflow.runtime.core.scheduler.cycle;

import org.apache.geaflow.core.graph.CycleGroupMeta;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionGraph;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.testng.annotations.Test;

public class ExecutionCycleBuilderTest {

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "cycle flyingCount should be positive, current value.*")
    public void validateGraphCycleFlyingCount() {
        ExecutionGraph executionGraph = new ExecutionGraph();
        CycleGroupMeta meta = executionGraph.getCycleGroupMeta();
        meta.setFlyingCount(0);
        meta.setGroupType(CycleGroupType.windowed);

        ExecutionCycleBuilder.buildExecutionCycle(executionGraph, null, null, 0, 0, null, 0, null, 0, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "cycle iterationCount should be positive, current value.*")
    public void validateGraphCycleIterationCount() {
        ExecutionGraph executionGraph = new ExecutionGraph();
        CycleGroupMeta meta = executionGraph.getCycleGroupMeta();
        meta.setIterationCount(0);
        meta.setGroupType(CycleGroupType.windowed);
        ExecutionCycleBuilder.buildExecutionCycle(executionGraph, null, null, 0, 0, null, 0, null, 0, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "cycle flyingCount should be positive, current value.*")
    public void validateNodeCycleFlyingCount() {
        ExecutionGraph executionGraph = new ExecutionGraph();
        CycleGroupMeta meta = executionGraph.getCycleGroupMeta();
        meta.setIterationCount(1);
        meta.setFlyingCount(1);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(0);
        vertexGroup.getCycleGroupMeta().setIterationCount(1);
        executionGraph.getVertexGroupMap().put(1, vertexGroup);

        ExecutionCycleBuilder.buildExecutionCycle(executionGraph, null, null, 0, 0, null, 0, null,
            0, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "cycle iterationCount should be positive, current value.*")
    public void validateNodeCycleIterationCount() {
        ExecutionGraph executionGraph = new ExecutionGraph();
        CycleGroupMeta meta = executionGraph.getCycleGroupMeta();
        meta.setIterationCount(1);
        meta.setFlyingCount(1);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(0);
        executionGraph.getVertexGroupMap().put(1, vertexGroup);

        ExecutionCycleBuilder.buildExecutionCycle(executionGraph, null, null, 0, 0, null, 0, null,
            0, false);
    }
}
