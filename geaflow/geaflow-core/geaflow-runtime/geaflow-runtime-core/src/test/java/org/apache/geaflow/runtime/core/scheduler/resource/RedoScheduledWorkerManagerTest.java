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
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RedoScheduledWorkerManagerTest extends BaseScheduledWorkerManagerTest {

    @Test
    public void testRequestMultiTimes() {
        int parallelism = 3;
        ExecutionGraphCycle cycle = buildMockCycle(parallelism);
        RedoCycleScheduledWorkerManager wm = new RedoCycleScheduledWorkerManager(new Configuration());
        RedoCycleScheduledWorkerManager workerManager = Mockito.spy(wm);

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
        Assert.assertEquals(parallelism,
            workerManager.workers.get(cycle.getSchedulerId()).getWorkers().size());
        workerManager.workers.remove(cycle.getSchedulerId());
    }
}
