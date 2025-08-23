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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CheckpointScheduledWorkerManagerTest extends BaseScheduledWorkerManagerTest {

    @Test
    public void testRequestMultiTimes() {
        int parallelism = 3;
        ExecutionGraphCycle cycle = buildMockCycle(parallelism);
        CheckpointCycleScheduledWorkerManager wm = new CheckpointCycleScheduledWorkerManager(new Configuration());
        CheckpointCycleScheduledWorkerManager workerManager = Mockito.spy(wm);

        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            workers.add(new WorkerInfo("", 0, 0, 0, i, "worker-" + i));
        }
        String resourceId = workerManager.genResourceId(cycle.getDriverIndex(),
            cycle.getSchedulerId());
        Mockito.doReturn(workers).when(workerManager).requestWorker(anyInt(), eq(resourceId));
        Mockito.doNothing().when(workerManager).initWorkers(anyLong(), any(), any());
        workerManager.init(cycle);
        Assert.assertEquals(parallelism,
            workerManager.workers.get(cycle.getSchedulerId()).getWorkers().size());

        workerManager.assign(cycle);
        Assert.assertEquals(parallelism, workerManager.workers.get(cycle.getSchedulerId()).getWorkers().size());
        workerManager.workers.remove(cycle.getSchedulerId());
    }

    @Test
    public void testTaskAssigner() throws Exception {
        List<WorkerInfo> workers = new ArrayList<>();
        int taskSize = 3;
        for (int i = 0; i < taskSize; i++) {
            workers.add(new WorkerInfo("", 0, 0, 0, i, "worker-" + i));
        }
        List<Integer> taskIndexes = new ArrayList<>();
        for (int i = 0; i < taskSize; i++) {
            taskIndexes.add(i);
        }
        String graphName = "test1";
        TaskAssigner taskAssigner = new TaskAssigner();

        // Current worker list: [0, 1, 2].
        // Assign: t0:[w0], t1:[w1], t2:[w2].
        Map<Integer, WorkerInfo> match0 = taskAssigner.assignTasks2Workers(graphName, taskIndexes,
            workers);

        // Hit all cache.
        Map<Integer, WorkerInfo> match1 = taskAssigner.assignTasks2Workers(graphName, taskIndexes,
            workers);
        for (Integer taskIndex : taskIndexes) {
            Assert.assertEquals(match0.get(taskIndex).getWorkerIndex(),
                match1.get(taskIndex).getWorkerIndex());
        }

        // Current worker list: [0, 1, 3].
        // Assign: t0:[w0], t1:[w1], t2:[w2, w3].
        WorkerInfo worker2 = workers.remove(taskSize - 1);
        workers.add(new WorkerInfo("", 0, 0, 0, taskSize, "worker-" + taskSize));
        Map<Integer, WorkerInfo> match2 = taskAssigner.assignTasks2Workers(graphName, taskIndexes,
            workers);
        Integer task_index1 = 0;
        for (Integer taskIndex : taskIndexes) {
            if (match1.get(taskIndex).getWorkerIndex() == taskSize - 1) {
                Assert.assertEquals(taskSize, match2.get(taskIndex).getWorkerIndex());
            } else {
                if (match1.get(taskIndex).getWorkerIndex() == 1) {
                    task_index1 = taskIndex;
                }
                Assert.assertEquals(match1.get(taskIndex).getWorkerIndex(),
                    match2.get(taskIndex).getWorkerIndex());
            }
        }

        // Current worker list: [1, 2, 3].
        // Assign: t0:[w0, w2 or w3], t1:[w1], t2:[w2, w3].
        workers.remove(0);
        workers.add(worker2);
        Map<Integer, WorkerInfo> match3 = taskAssigner.assignTasks2Workers(graphName, taskIndexes,
            workers);
        for (Integer taskIndex : taskIndexes) {
            if (match3.get(taskIndex).getWorkerIndex() == 1) {
                Assert.assertEquals(taskIndex, task_index1);
            }
        }

        // Current worker list: [2, 3, 4]
        // Assign: t0:[w0, w2 or w3], t1:[w1, w4], t2:[w2, w3]
        workers.remove(0);
        workers.add(new WorkerInfo("", 0, 0, 0, taskSize + 1, "worker-" + taskSize + 1));
        Map<Integer, WorkerInfo> match4 = taskAssigner.assignTasks2Workers(graphName, taskIndexes,
            workers);
        for (Integer taskIndex : taskIndexes) {
            if (match3.get(taskIndex).getWorkerIndex() == 1) {
                Assert.assertEquals(taskIndex, task_index1);
            }
        }
    }

}
