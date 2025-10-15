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

package org.apache.geaflow.cluster.resourcemanager;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_WORKER_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.ClusterId;
import org.apache.geaflow.cluster.clustermanager.ContainerExecutorInfo;
import org.apache.geaflow.cluster.clustermanager.ExecutorRegisteredCallback;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.config.ClusterConfig;
import org.apache.geaflow.cluster.container.ContainerInfo;
import org.apache.geaflow.cluster.master.MasterContext;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.state.StoreType;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultResourceManagerTest {

    private static final String TEST = "test";

    private static final ExecutorService POOL = Executors.newCachedThreadPool();
    private Configuration config = new Configuration();

    @BeforeMethod
    public void setUp() {
        config = new Configuration();
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        ClusterMetaStore.close();
    }

    @Test
    public void testAllocateWithWorkerEqUserDefinedWorker() {
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow12345");
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 2);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 0);
    }

    @Test
    public void testAllocateWithWorkerGtUserDefinedWorker() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 10);

        RequireResourceRequest request3 = RequireResourceRequest.build(TEST + 3, 10);
        RequireResponse response3 = resourceManager.requireResource(request3);
        Assert.assertEquals(response3.getWorkers().size(), 0);
    }

    @Test
    public void testAllocateWithWorkerLtUserDefinedWorker() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_NUM.getKey(), String.valueOf(4));
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 10);

        RequireResourceRequest request3 = RequireResourceRequest.build(TEST + 3, 6);
        RequireResponse response3 = resourceManager.requireResource(request3);
        Assert.assertEquals(response3.getWorkers().size(), 6);

        RequireResourceRequest request4 = RequireResourceRequest.build(TEST + 4, 1);
        RequireResponse response4 = resourceManager.requireResource(request4);
        Assert.assertEquals(response4.getWorkers().size(), 0);
    }

    @Test
    public void testAllocateAndRelease() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 10);

        RequireResourceRequest request3 = RequireResourceRequest.build(TEST + 3, 6);
        RequireResponse response3 = resourceManager.requireResource(request3);
        Assert.assertEquals(response3.getWorkers().size(), 6);

        RequireResourceRequest request4 = RequireResourceRequest.build(TEST + 4, 1);
        RequireResponse response4 = resourceManager.requireResource(request4);
        Assert.assertEquals(response4.getWorkers().size(), 0);

        resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 1, response1.getWorkers()));
        resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 2, response2.getWorkers()));

        RequireResourceRequest request5 = RequireResourceRequest.build(TEST + 5, 15);
        RequireResponse response5 = resourceManager.requireResource(request5);
        Assert.assertEquals(response5.getWorkers().size(), 0);

        resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 3, response3.getWorkers()));
        RequireResourceRequest request6 = RequireResourceRequest.build(TEST + 6, 15);
        RequireResponse response6 = resourceManager.requireResource(request6);
        Assert.assertEquals(response6.getWorkers().size(), 15);
    }

    @Test
    public void testAllocateAndReleaseFail() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        ReleaseResponse releaseRes1 = resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 1, response1.getWorkers().subList(0, 1)));
        Assert.assertFalse(releaseRes1.isSuccess());

        ReleaseResponse releaseRes2 = resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 1, response1.getWorkers()));
        Assert.assertTrue(releaseRes2.isSuccess());

        ReleaseResponse releaseRes3 = resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 3, response1.getWorkers()));
        Assert.assertFalse(releaseRes3.isSuccess());


    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testReleaseException() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 4);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 4);

        resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 1, response2.getWorkers()));
    }

    @Test
    public void testAllocateWithIllegalNum() {
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow12345");
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST, -1);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertFalse(response1.isSuccess());
    }

    @Test
    public void testReleaseWithIllegalResourceId() {
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow12345");
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST, -1);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertFalse(response1.isSuccess());

        ReleaseResponse response2 = resourceManager.releaseResource(ReleaseResourceRequest.build(TEST + 1, response1.getWorkers()));
        Assert.assertFalse(response2.isSuccess());
    }

    @Test
    public void testAllocateWithOneRequireId() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertTrue(response1.isSuccess());
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST, 4);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertTrue(response2.isSuccess());
        Assert.assertEquals(response2.getWorkers().size(), 4);
    }

    @Test
    public void testAllocateWithOneRequireIdFail() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockClusterManager clusterManager = new MockClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST, 4);
        RequireResponse response1 = resourceManager.requireResource(request1);
        Assert.assertTrue(response1.isSuccess());
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST, 10);
        RequireResponse response2 = resourceManager.requireResource(request2);
        Assert.assertFalse(response2.isSuccess());
    }

    @Test
    public void testRecoverWithWorkerGtUserDefinedWorker() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockRecoverClusterManager clusterManager = new MockRecoverClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        clusterContext.getCallbacks().clear();

        DefaultResourceManager recoverRm = new DefaultResourceManager(clusterManager);
        MasterContext recoverMasterContext = new MasterContext(config);
        clusterContext.setRecover(true);
        recoverRm.init(ResourceManagerContext.build(recoverMasterContext, clusterContext));

        pending = recoverRm.getPendingWorkerCounter();
        lock = recoverRm.getResourceLock();
        // wait async allocate worker ready
        do {
            SleepUtils.sleepMilliSecond(10);
        } while (pending.get() > 0 || !lock.get());

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = recoverRm.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = recoverRm.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 10);

        RequireResourceRequest request3 = RequireResourceRequest.build(TEST + 3, 10);
        RequireResponse response3 = recoverRm.requireResource(request3);
        Assert.assertEquals(response3.getWorkers().size(), 0);
    }

    @Test
    public void testRecoverWithWorkerLtUserDefinedWorker() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        config.put(CONTAINER_NUM.getKey(), String.valueOf(4));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockRecoverClusterManager clusterManager = new MockRecoverClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        clusterContext.getCallbacks().clear();

        DefaultResourceManager recoverRm = new DefaultResourceManager(clusterManager);
        MasterContext recoverMasterContext = new MasterContext(config);
        clusterContext.setRecover(true);
        recoverRm.init(ResourceManagerContext.build(recoverMasterContext, clusterContext));

        pending = recoverRm.getPendingWorkerCounter();
        lock = recoverRm.getResourceLock();
        // wait async allocate worker ready
        do {
            SleepUtils.sleepMilliSecond(10);
        } while (pending.get() > 0 || !lock.get());

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = recoverRm.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = recoverRm.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 10);

        RequireResourceRequest request3 = RequireResourceRequest.build(TEST + 3, 6);
        RequireResponse response3 = recoverRm.requireResource(request3);
        Assert.assertEquals(response3.getWorkers().size(), 6);

        RequireResourceRequest request4 = RequireResourceRequest.build(TEST + 4, 1);
        RequireResponse response4 = recoverRm.requireResource(request4);
        Assert.assertEquals(response4.getWorkers().size(), 0);
    }

    @Test
    public void testUseSomeAndRecover() {
        config.put(JOB_UNIQUE_ID.getKey(), "geaflow23456");
        config.put(CONTAINER_NUM.getKey(), String.valueOf(4));
        config.put(CONTAINER_WORKER_NUM.getKey(), String.valueOf(5));
        ClusterMetaStore.init(0, "master-0", config);
        ClusterContext clusterContext = new ClusterContext(config);
        MockRecoverClusterManager clusterManager = new MockRecoverClusterManager();
        clusterManager.init(clusterContext);

        DefaultResourceManager resourceManager = new DefaultResourceManager(clusterManager);
        resourceManager.init(ResourceManagerContext.build(new MasterContext(config), clusterContext));

        AtomicInteger pending = resourceManager.getPendingWorkerCounter();
        AtomicBoolean lock = resourceManager.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request0 = RequireResourceRequest.build(TEST + 0, 7);
        RequireResponse response0 = resourceManager.requireResource(request0);
        Assert.assertEquals(response0.getWorkers().size(), 7);

        clusterContext.getCallbacks().clear();

        DefaultResourceManager recoverRm = new DefaultResourceManager(clusterManager);
        MasterContext recoverMasterContext = new MasterContext(config);
        clusterContext.setRecover(true);
        recoverRm.init(ResourceManagerContext.build(recoverMasterContext, clusterContext));

        pending = recoverRm.getPendingWorkerCounter();
        lock = recoverRm.getResourceLock();
        while (pending.get() > 0 || !lock.get()) {
            SleepUtils.sleepMilliSecond(10);
        }

        RequireResourceRequest request1 = RequireResourceRequest.build(TEST + 1, 4);
        RequireResponse response1 = recoverRm.requireResource(request1);
        Assert.assertEquals(response1.getWorkers().size(), 4);

        RequireResourceRequest request2 = RequireResourceRequest.build(TEST + 2, 10);
        RequireResponse response2 = recoverRm.requireResource(request2);
        Assert.assertEquals(response2.getWorkers().size(), 0);
    }

    @Test
    public void testPersistWorkers() {
        List<WorkerInfo> available = buildMockWorkers(buildMockContainerExecutorInfo(0, 10));
        List<WorkerInfo> used = buildMockWorkers(buildMockContainerExecutorInfo(1, 10));
        ResourceSession session = new ResourceSession(TEST);
        for (WorkerInfo worker : used) {
            session.addWorker(worker.generateWorkerId(), worker);
        }
        List<ResourceSession> sessions = Collections.singletonList(session);

        WorkerSnapshot workerSnapshot = new WorkerSnapshot();
        workerSnapshot.setAvailableWorkers(available);
        workerSnapshot.setSessions(sessions);
        Assert.assertNotNull(workerSnapshot.getAvailableWorkers());
        Assert.assertNotNull(workerSnapshot.getSessions());
        Assert.assertEquals(workerSnapshot.getAvailableWorkers().size(), 10);
        Assert.assertEquals(workerSnapshot.getSessions().size(), 1);
        Assert.assertEquals(workerSnapshot.getSessions().get(0).getWorkers().size(), 10);
        // test serialize
        ISerializer kryoSerializer = SerializerFactory.getKryoSerializer();
        byte[] bytes = kryoSerializer.serialize(workerSnapshot);
        WorkerSnapshot deserialized = (WorkerSnapshot) kryoSerializer.deserialize(bytes);
        List<WorkerInfo> deAvailable = deserialized.getAvailableWorkers();
        List<ResourceSession> deSessions = deserialized.getSessions();
        Assert.assertNotNull(deAvailable);
        Assert.assertNotNull(deSessions);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(deAvailable.get(i).getContainerName(), "host0");
            Assert.assertEquals(deAvailable.get(i).getHost(), "host0");
            Assert.assertEquals(deAvailable.get(i).getWorkerIndex(), i);
        }
        List<WorkerInfo> deUsed = new ArrayList<>(deSessions.get(0).getWorkers().values());
        deUsed.sort(Comparator.comparing(WorkerInfo::getWorkerIndex));
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(deUsed.get(i).getContainerName(), "host1");
            Assert.assertEquals(deUsed.get(i).getHost(), "host1");
            Assert.assertEquals(deUsed.get(i).getWorkerIndex(), i);
        }

    }

    @AfterTest
    public void destroy() {
        POOL.shutdown();
    }

    private static ContainerExecutorInfo buildMockContainerExecutorInfo(int hostId, int workerNum) {
        String host = "host" + hostId;
        return new ContainerExecutorInfo(new ContainerInfo(hostId, host, host, 0, 0, 0), 0, workerNum);
    }

    private static List<WorkerInfo> buildMockWorkers(ContainerExecutorInfo container) {
        String containerName = container.getContainerName();
        String host = container.getHost();
        int rpcPort = container.getRpcPort();
        int shufflePort = container.getShufflePort();
        int processId = container.getProcessId();
        List<Integer> executorIds = container.getExecutorIds();
        List<WorkerInfo> workers = new ArrayList<>();
        for (Integer workerIndex : executorIds) {
            WorkerInfo worker = WorkerInfo.build(
                host, rpcPort, shufflePort, processId, 0, workerIndex, containerName);
            workers.add(worker);
        }
        return workers;
    }

    private static class MockClusterManager implements IClusterManager {

        protected final AtomicInteger counter = new AtomicInteger();
        protected ClusterContext clusterContext;
        protected int containerWorkerNum;

        @Override
        public void init(ClusterContext context) {
            this.clusterContext = context;
            this.clusterContext.getClusterConfig().getConfig().put(SYSTEM_STATE_BACKEND_TYPE.getKey(),
                StoreType.MEMORY.name());
            ClusterConfig clusterConfig = ClusterConfig.build(context.getConfig());
            this.containerWorkerNum = clusterConfig.getContainerWorkerNum();
        }

        @Override
        public ClusterId startMaster() {
            return null;
        }

        @Override
        public Map<String, ConnectAddress> startDrivers() {
            return null;
        }

        @Override
        public void allocateWorkers(int executorNum) {
            POOL.execute(() -> {
                int left = executorNum;
                while (left > 0) {
                    for (ExecutorRegisteredCallback callback : this.clusterContext.getCallbacks()) {
                        callback.onSuccess(buildMockContainerExecutorInfo(this.counter.getAndIncrement(), this.containerWorkerNum));
                    }
                    left -= this.containerWorkerNum;
                }
            });
        }

        @Override
        public void doFailover(int componentId, Throwable cause) {
        }

        @Override
        public void close() {
        }
    }

    private static class MockRecoverClusterManager extends MockClusterManager {

        @Override
        public void allocateWorkers(int executorNum) {
            POOL.execute(() -> {
                int left = executorNum;
                int hostId = 0;
                while (left > 0) {
                    for (ExecutorRegisteredCallback callback : this.clusterContext.getCallbacks()) {
                        callback.onSuccess(buildMockContainerExecutorInfo(hostId, this.containerWorkerNum));
                    }
                    left -= this.containerWorkerNum;
                    hostId++;
                }
            });
        }
    }

}
