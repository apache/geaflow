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

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.ContainerExecutorInfo;
import org.apache.geaflow.cluster.clustermanager.ExecutorRegisterException;
import org.apache.geaflow.cluster.clustermanager.ExecutorRegisteredCallback;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.resourcemanager.allocator.IAllocator;
import org.apache.geaflow.cluster.resourcemanager.allocator.ProcessFairAllocator;
import org.apache.geaflow.cluster.resourcemanager.allocator.RoundRobinAllocator;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.cluster.web.metrics.ResourceMetrics;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.SleepUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultResourceManager implements IResourceManager, ExecutorRegisteredCallback, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResourceManager.class);

    private static final String OPERATION_REQUIRE = "require";
    private static final String OPERATION_RELEASE = "release";
    private static final String OPERATION_ALLOCATE = "allocate";

    private static final int DEFAULT_SLEEP_MS = 10;
    private static final int MAX_REQUIRE_RETRY_TIMES = 1;
    private static final int MAX_RELEASE_RETRY_TIMES = 100;

    private final AtomicReference<Throwable> allocateWorkerErr = new AtomicReference<>();
    private final AtomicInteger pendingWorkerCounter = new AtomicInteger(0);
    private final AtomicBoolean resourceLock = new AtomicBoolean(true);
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final AtomicBoolean inited = new AtomicBoolean(false);
    private final Map<IAllocator.AllocateStrategy, IAllocator<String, WorkerInfo>> allocators = new HashMap<>();
    protected final IClusterManager clusterManager;
    protected final ClusterMetaStore metaKeeper;
    protected int totalWorkerNum;

    private final Map<WorkerInfo.WorkerId, WorkerInfo> availableWorkers = new TreeMap<>(
        Comparator.comparing(WorkerInfo.WorkerId::getContainerName).thenComparing(WorkerInfo.WorkerId::getWorkerIndex));
    private final Map<String, ResourceSession> sessions = new HashMap<>();

    public DefaultResourceManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.metaKeeper = ClusterMetaStore.getInstance();
    }

    @Override
    public void init(ResourceManagerContext context) {
        this.allocators.put(IAllocator.AllocateStrategy.ROUND_ROBIN, new RoundRobinAllocator());
        this.allocators.put(IAllocator.AllocateStrategy.PROCESS_FAIR, new ProcessFairAllocator());
        ClusterContext clusterContext = context.getClusterContext();
        clusterContext.addExecutorRegisteredCallback(this);

        boolean isRecover = context.isRecover();
        this.recovering.set(isRecover);
        int workerNum = clusterContext.getClusterConfig().getContainerNum()
            * clusterContext.getClusterConfig().getContainerWorkerNum();
        this.totalWorkerNum += workerNum;
        this.pendingWorkerCounter.set(workerNum);
        LOGGER.info("init worker number {}, isRecover {}", workerNum, isRecover);
        if (isRecover) {
            this.recover();
        } else {
            this.clusterManager.allocateWorkers(workerNum);
        }
        this.inited.set(true);
        LOGGER.info("init worker manager finish");
    }

    @Override
    public RequireResponse requireResource(RequireResourceRequest requireRequest) {
        String requireId = requireRequest.getRequireId();
        int requiredNum = requireRequest.getRequiredNum();
        if (this.sessions.containsKey(requireId)) {
            Map<WorkerInfo.WorkerId, WorkerInfo> sessionWorkers = this.sessions.get(requireId).getWorkers();
            if (requiredNum != sessionWorkers.size()) {
                String msg = "require number mismatch, old " + sessionWorkers.size() + " new " + requiredNum;
                LOGGER.error("[{}] require from session err: {}", requireId, msg);
                return RequireResponse.fail(requireId, msg);
            }
            List<WorkerInfo> workers = new ArrayList<>(sessionWorkers.values());
            LOGGER.info("[{}] require from session with {} worker", requireId, workers.size());
            return RequireResponse.success(requireId, workers);
        }
        if (requiredNum <= 0) {
            String msg = RuntimeErrors.INST.resourceIllegalRequireNumError("illegal num " + requiredNum);
            LOGGER.error("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }
        if (this.recovering.get()) {
            String msg = "resource manager still recovering";
            LOGGER.warn("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }
        if (this.pendingWorkerCounter.get() > 0) {
            String msg = "some worker still pending creation";
            LOGGER.warn("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }

        Optional<List<WorkerInfo>> optional = this.withLock(OPERATION_REQUIRE, num -> {
            if (this.availableWorkers.size() < num) {
                LOGGER.warn("[{}] require {}, available {}, return empty",
                    requireId, num, this.availableWorkers.size());
                return Collections.emptyList();
            }
            IAllocator.AllocateStrategy strategy = requireRequest.getAllocateStrategy();
            List<WorkerInfo> allocated = this.allocators.get(strategy)
                .allocate(this.availableWorkers.values(), num);
            for (WorkerInfo worker : allocated) {
                WorkerInfo.WorkerId workerId = worker.generateWorkerId();
                this.availableWorkers.remove(workerId);
                ResourceSession session = this.sessions.computeIfAbsent(requireId, ResourceSession::new);
                session.addWorker(workerId, worker);
            }
            LOGGER.info("[{}] require {} allocated {} available {}",
                requireId, num, allocated.size(), this.availableWorkers.size());
            if (!allocated.isEmpty()) {
                this.persist();
            }
            return allocated;
        }, requiredNum, MAX_REQUIRE_RETRY_TIMES);
        List<WorkerInfo> allocated = optional.orElse(Collections.emptyList());
        return RequireResponse.success(requireId, allocated);
    }

    @Override
    public ReleaseResponse releaseResource(ReleaseResourceRequest releaseRequest) {
        String releaseId = String.valueOf(releaseRequest.getReleaseId());
        if (!this.sessions.containsKey(releaseId)) {
            String msg = "release fail, session not exists: " + releaseId;
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        int expectSize = this.sessions.get(releaseId).getWorkers().size();
        int actualSize = releaseRequest.getWorkers().size();
        if (expectSize != actualSize) {
            String msg = String.format("release fail, worker num of session %s mismatch, expected %d, actual %d",
                releaseId, expectSize, actualSize);
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        Optional<Boolean> optional = this.withLock(OPERATION_RELEASE, workers -> {
            for (WorkerInfo worker : workers) {
                WorkerInfo.WorkerId workerId = worker.generateWorkerId();
                this.availableWorkers.put(workerId, worker);
                if (!this.sessions.get(releaseId).removeWorker(workerId)) {
                    String msg = String.format("worker %s not exists in session %s", workerId, releaseId);
                    LOGGER.error(msg);
                    throw new GeaflowRuntimeException(msg);
                }
            }
            this.sessions.remove(releaseId);
            LOGGER.info("[{}] release {} available {}",
                releaseId, workers.size(), this.availableWorkers.size());
            this.persist();
            return true;
        }, releaseRequest.getWorkers(), MAX_RELEASE_RETRY_TIMES);

        if (!optional.orElse(false)) {
            String msg = "release fail after " + MAX_RELEASE_RETRY_TIMES + " times";
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        return ReleaseResponse.success(releaseId);
    }

    @Override
    public void onSuccess(ContainerExecutorInfo containerExecutorInfo) {
        this.waitForInit();
        this.withLock(OPERATION_ALLOCATE, container -> {
            String containerName = container.getContainerName();
            String host = container.getHost();
            int rpcPort = container.getRpcPort();
            int shufflePort = container.getShufflePort();
            int processId = container.getProcessId();
            List<Integer> executorIds = container.getExecutorIds();
            onRegister(containerName, host, rpcPort, shufflePort, processId, executorIds);
            return true;
        }, containerExecutorInfo, Integer.MAX_VALUE);
    }

    private void onRegister(String containerName,
                            String host,
                            int rpcPort,
                            int shufflePort,
                            int processId,
                            List<Integer> executorIds) {
        for (Integer workerIndex : executorIds) {
            WorkerInfo.WorkerId workerId = new WorkerInfo.WorkerId(containerName, workerIndex);
            WorkerInfo worker = null;
            if (this.availableWorkers.containsKey(workerId)) {
                worker = this.availableWorkers.get(workerId);
            }
            for (ResourceSession session : this.sessions.values()) {
                if (session.getWorkers().containsKey(workerId)) {
                    worker = session.getWorkers().get(workerId);
                }
            }
            if (worker == null) {
                worker = WorkerInfo.build(
                    host, rpcPort, shufflePort, processId, workerIndex, containerName);
                this.availableWorkers.put(worker.generateWorkerId(), worker);
                this.pendingWorkerCounter.addAndGet(-1);
            } else {
                worker.setHost(host);
                worker.setProcessId(processId);
                worker.setRpcPort(rpcPort);
                worker.setShufflePort(shufflePort);
            }
        }
        int pending = this.pendingWorkerCounter.get();
        LOGGER.info("register {} worker from cluster manager container:{}, host:{}, processId:{},"
            + " pending:{}", executorIds.size(), containerName, host, processId, pending);

        int used = this.sessions.values().stream().mapToInt(s -> s.getWorkers().size()).sum();
        if (pending <= 0) {
            this.recovering.set(false);
            LOGGER.info("register worker over, available/used : {}/{}, pending {}",
                this.availableWorkers.size(), used, pending);
            persist();
        } else {
            LOGGER.debug("still pending : {}, available/used : {}/{}", pending,
                this.availableWorkers.size(), used);
        }
    }

    @Override
    public void onFailure(ExecutorRegisterException e) {
        LOGGER.error("create worker err", e);
        this.allocateWorkerErr.compareAndSet(null, e);
    }

    private <T, R> Optional<R> withLock(String operation, Function<T, R> function, T input, int maxRetryTimes) {
        this.checkError();
        try {
            int retry = 0;
            while (!this.resourceLock.compareAndSet(true, false)) {
                SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
                retry++;
                if (retry >= maxRetryTimes) {
                    LOGGER.warn("[{}] lock not ready, return empty", operation);
                    return Optional.empty();
                }
                if (retry % 100 == 0) {
                    LOGGER.warn("[{}] lock not ready after {} times", operation, retry);
                }
            }
            return Optional.of(function.apply(input));
        } finally {
            this.resourceLock.set(true);
        }
    }

    private void persist() {
        final long start = System.currentTimeMillis();
        List<WorkerInfo> available = new ArrayList<>(this.availableWorkers.values());
        List<ResourceSession> sessions = new ArrayList<>(this.sessions.values());
        int used = sessions.stream().mapToInt(s -> s.getWorkers().size()).sum();
        this.metaKeeper.saveWorkers(new WorkerSnapshot(available, sessions));
        LOGGER.info("persist {}/{} workers costs {}ms",
            this.availableWorkers.size(), used, System.currentTimeMillis() - start);
    }

    private void recover() {
        final long start = System.currentTimeMillis();
        WorkerSnapshot workerSnapshot = this.metaKeeper.getWorkers();
        List<WorkerInfo> available = workerSnapshot.getAvailableWorkers();
        List<ResourceSession> sessions = workerSnapshot.getSessions();
        for (WorkerInfo worker : available) {
            WorkerInfo.WorkerId workerId = worker.generateWorkerId();
            this.availableWorkers.put(workerId, worker);
        }
        int usedWorkerNum = 0;
        for (ResourceSession session : sessions) {
            String sessionId = session.getId();
            this.sessions.put(sessionId, session);
            usedWorkerNum += session.getWorkers().size();
        }
        int availableWorkerNum = this.availableWorkers.size();

        this.pendingWorkerCounter.addAndGet(-availableWorkerNum - usedWorkerNum);
        LOGGER.info("recover {}/{} workers, pending {}, costs {}ms", availableWorkerNum,
            usedWorkerNum, pendingWorkerCounter.get(), System.currentTimeMillis() - start);
        if (this.pendingWorkerCounter.get() <= 0) {
            this.recovering.set(false);
            LOGGER.info("recover worker over, available/used : {}/{}", this.availableWorkers.size(),
                usedWorkerNum);
        }
        clusterManager.doFailover(ClusterConstants.DEFAULT_MASTER_ID, null);
    }

    private void waitForInit() {
        int count = 0;
        while (!this.inited.get()) {
            count++;
            if (count % 100 == 0) {
                LOGGER.warn("resource manager not inited, wait {}ms and retry", DEFAULT_SLEEP_MS);
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }
    }

    public ResourceMetrics getResourceMetrics() {
        ResourceMetrics metrics = new ResourceMetrics();
        metrics.setPendingWorkers(pendingWorkerCounter.get());
        metrics.setAvailableWorkers(availableWorkers.size());
        metrics.setTotalWorkers(totalWorkerNum);
        return metrics;
    }

    @VisibleForTesting
    protected AtomicInteger getPendingWorkerCounter() {
        return this.pendingWorkerCounter;
    }

    @VisibleForTesting
    protected AtomicBoolean getResourceLock() {
        return this.resourceLock;
    }

    private void checkError() {
        Throwable firstException = this.allocateWorkerErr.get();
        if (firstException != null) {
            throw new GeaflowRuntimeException(firstException);
        }
    }

}
