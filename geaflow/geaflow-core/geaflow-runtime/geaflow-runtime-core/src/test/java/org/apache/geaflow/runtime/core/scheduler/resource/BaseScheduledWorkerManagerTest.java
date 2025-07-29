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

package org.apache.geaflow.runtime.core.scheduler.resource;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.state.StoreType;
import org.testng.annotations.AfterMethod;

public abstract class BaseScheduledWorkerManagerTest {

    protected ExecutionGraphCycle buildMockCycle(int parallelism) {
        Configuration configuration = new Configuration();
        configuration.put(JOB_UNIQUE_ID, "test-scheduler-context");
        configuration.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        ClusterMetaStore.init(0, "driver-0", configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        int taskNum = parallelism;
        vertex.setParallelism(taskNum);
        vertexGroup.getVertexMap().put(0, vertex);
        List<ExecutionTask> tasks = new ArrayList<>();
        for (int i = 0; i < taskNum; i++) {
            ExecutionTask task = new ExecutionTask(i, i, taskNum, taskNum, taskNum, 0);
            tasks.add(task);
        }
        ExecutionNodeCycle cycle = new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup,
            configuration, "driver_id", 0);
        cycle.setTasks(tasks);

        return new ExecutionGraphCycle(0, 0, 0, "test", 0, 1, 1, configuration, "driverId", 0);
    }

    @AfterMethod
    public void afterMethod() {
        ScheduledWorkerManagerFactory.clear();
    }

}
