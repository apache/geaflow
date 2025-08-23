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

import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseCycleSchedulerContextTest {

    protected static MockedStatic<ScheduledWorkerManagerFactory> mockWorkerManager;
    protected Configuration configuration = new Configuration();

    @BeforeMethod
    public void setUp() {
        ClusterMetaStore.close();
        mockWorkerManager = mockScheduledWorkerManager();

        String path = "/tmp/" + this.getClass().getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), this.getClass().getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.JOB_UNIQUE_ID, "test-scheduler-context");
    }

    @AfterMethod
    public void cleanUp() {
        mockWorkerManager.close();
        String path = "/tmp/" + this.getClass().getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        ClusterMetaStore.close();
    }

    public static MockedStatic<ScheduledWorkerManagerFactory> mockScheduledWorkerManager() {
        MockedStatic<ScheduledWorkerManagerFactory> ms = Mockito.mockStatic(ScheduledWorkerManagerFactory.class);
        ms.when(() ->
                ScheduledWorkerManagerFactory.createScheduledWorkerManager(any(), any()))
            .then(invocation -> new MockScheduledWorkerManager());
        return ms;
    }

    protected static class MockScheduledWorkerManager
        implements IScheduledWorkerManager<ExecutionGraphCycle> {

        @Override
        public void init(ExecutionGraphCycle graph) {
        }

        @Override
        public List<WorkerInfo> assign(ExecutionGraphCycle vertex) {
            return null;
        }

        @Override
        public void release(ExecutionGraphCycle vertex) {
        }

        @Override
        public void clean(CleanWorkerFunction cleaFunc, IExecutionCycle cycle) {
        }

        @Override
        public void close(IExecutionCycle cycle) {
        }
    }
}
