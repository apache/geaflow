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

package org.apache.geaflow.cluster.rpc;

import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.CONTAINER;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.DRIVER;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.MASTER;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.METRIC;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.PIPELINE_MANAGER;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.RESOURCE_MANAGER;
import static org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType.SUPERVISOR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.RPC_ASYNC_THREADS;

import com.google.protobuf.Empty;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.ReleaseResponse;
import org.apache.geaflow.cluster.resourcemanager.RequireResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.RequireResponse;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType;
import org.apache.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.DefaultRpcCallbackImpl;
import org.apache.geaflow.cluster.rpc.impl.DriverEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.MasterEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.MetricEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.PipelineMasterEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.ResourceManagerEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.SupervisorEndpointRef;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.ha.service.AbstractHAService;
import org.apache.geaflow.ha.service.HAServiceFactory;
import org.apache.geaflow.ha.service.IHAService;
import org.apache.geaflow.ha.service.ResourceData;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.rpc.proto.Container.Response;
import org.apache.geaflow.rpc.proto.Master.HeartbeatResponse;
import org.apache.geaflow.rpc.proto.Master.RegisterResponse;
import org.apache.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import org.apache.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import org.apache.geaflow.rpc.proto.Supervisor.StatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);
    private static final int RPC_RETRY_EXTRA_MS = 30000;
    private static IHAService haService;
    private static RpcEndpointRefFactory refFactory;
    private static RpcClient INSTANCE;
    private final int retryTimes;
    private final int retryIntervalMs;
    private final ExecutorService executorService;

    private RpcClient(Configuration configuration) {
        // Ensure total retry time be longer than (heartbeat timeout + 30s).
        retryIntervalMs = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_INTERVAL_MS);
        int heartbeatTimeoutMs = configuration.getInteger(HEARTBEAT_TIMEOUT_MS);
        int minTimes = (int) Math.ceil(
            (double) (heartbeatTimeoutMs + RPC_RETRY_EXTRA_MS) / retryIntervalMs);
        int rpcRetryTimes = configuration.getInteger(ExecutionConfigKeys.RPC_RETRY_TIMES);
        retryTimes = Math.max(minTimes, rpcRetryTimes);
        refFactory = RpcEndpointRefFactory.getInstance(configuration);
        haService = HAServiceFactory.getService(configuration);

        int threads = configuration.getInteger(RPC_ASYNC_THREADS);
        this.executorService = new ThreadPoolExecutor(threads, threads, Long.MAX_VALUE,
            TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
            ThreadUtil.namedThreadFactory(true, "rpc-executor"));

        LOGGER.info("RpcClient init retryTimes:{} retryIntervalMs:{} threads:{}", retryTimes,
            retryIntervalMs, threads);
    }

    public static synchronized RpcClient init(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new RpcClient(configuration);
        }
        return INSTANCE;
    }

    public static synchronized RpcClient getInstance() {
        return INSTANCE;
    }

    // Master endpoint ref.
    public <T> void registerContainer(String masterId, T info,
                                      RpcCallback<RegisterResponse> callback) {
        doRpcWithRetry(() -> {
            MasterEndpointRef endpointRef = connectMaster(masterId);
            if (endpointRef == null) {
                LOGGER.warn("Cannot register container with master {}: endpoint not available", masterId);
                return;
            }
            endpointRef.registerContainer(info, new DefaultRpcCallbackImpl<>(callback, masterId, haService));
        }, masterId, MASTER);
    }

    public void sendHeartBeat(String masterId, Heartbeat heartbeat,
                              RpcCallback<HeartbeatResponse> callback) {
        doRpcWithRetry(() -> {
            MasterEndpointRef endpointRef = connectMaster(masterId);
            if (endpointRef == null) {
                LOGGER.warn("Cannot send heartbeat to master {}: endpoint not available", masterId);
                return;
            }
            endpointRef.sendHeartBeat(heartbeat, new DefaultRpcCallbackImpl<>(callback, masterId, haService));
        }, masterId, MASTER);
    }

    public Empty sendException(String masterId, Integer containerId, String containerName,
                               Throwable throwable) {
        return doRpcWithRetry(() -> {
            MasterEndpointRef endpointRef = connectMaster(masterId);
            if (endpointRef == null) {
                LOGGER.warn("Cannot send exception to master {}: endpoint not available", masterId);
                return Empty.getDefaultInstance();
            }
            return endpointRef.sendException(containerId, containerName, throwable.getMessage());
        }, masterId, MASTER);
    }

    // Container endpoint ref.
    public Future processContainer(String containerId, IEvent event) {
        return doRpcWithRetry(() -> {
            ContainerEndpointRef endpointRef = connectContainer(containerId);
            if (endpointRef == null) {
                LOGGER.warn("Cannot process container event for {}: endpoint not available", containerId);
                return null;
            }
            return endpointRef.process(event, new DefaultRpcCallbackImpl(null, containerId, haService));
        }, containerId, CONTAINER);
    }

    public void processContainer(String containerId, IEvent event, RpcCallback<Response> callback) {
        doRpcWithRetry(() -> {
            ContainerEndpointRef endpointRef = connectContainer(containerId);
            if (endpointRef == null) {
                LOGGER.warn("Cannot process container event for {}: endpoint not available", containerId);
                return;
            }
            endpointRef.process(event, new DefaultRpcCallbackImpl<>(callback, containerId, haService));
        }, containerId, CONTAINER);
    }

    // Pipeline endpoint ref.
    public void processPipeline(String driverId, IEvent event) {
        doRpcWithRetry(
            () -> connectPipelineManager(driverId).process(event, new DefaultRpcCallbackImpl<>()),
            driverId, PIPELINE_MANAGER);
    }

    public IPipelineResult executePipeline(String driverId, Pipeline pipeline) {
        return doRpcWithRetry(() -> connectDriver(driverId).executePipeline(pipeline), driverId,
            DRIVER);
    }

    // Resource manager endpoint ref.
    public RequireResponse requireResource(String masterId, RequireResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).requireResource(request), masterId,
            RESOURCE_MANAGER);
    }

    public ReleaseResponse releaseResource(String masterId, ReleaseResourceRequest request) {
        return doRpcWithRetry(() -> connectRM(masterId).releaseResource(request), masterId,
            RESOURCE_MANAGER);
    }

    public Future<MetricQueryResponse> requestMetrics(String id, MetricQueryRequest request,
                                                      RpcCallback<MetricQueryResponse> callback) {
        return doRpcWithRetry(() -> connectMetricServer(id).queryMetrics(request,
            new DefaultRpcCallbackImpl<>(callback, id, haService)), id, METRIC);
    }

    public Future restartWorkerBySupervisor(String id, boolean fastFailure) {
        int retries = fastFailure ? 1 : retryTimes;
        try {
            return doRpcWithRetry(() -> {
                ResourceData resourceData = loadSupervisorData(id, fastFailure);
                return connectSupervisor(resourceData).restart(resourceData.getProcessId(),
                    new DefaultRpcCallbackImpl<>());
            }, id, SUPERVISOR, retries);
        } catch (Throwable e) {
            CompletableFuture<Empty> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }
    }

    public StatusResponse queryWorkerStatusBySupervisor(String id) {
        return connectSupervisor(id).status();
    }

    // Close endpoint connection.
    public void closeMasterConnection(String masterId) {
        MasterEndpointRef endpointRef = connectMaster(masterId);
        if (endpointRef != null) {
            endpointRef.closeEndpoint();
        } else {
            LOGGER.debug("No endpoint reference found for master: {}, skipping close", masterId);
        }
    }

    public void closeDriverConnection(String driverId) {
        DriverEndpointRef endpointRef = connectDriver(driverId);
        if (endpointRef != null) {
            endpointRef.closeEndpoint();
        } else {
            LOGGER.debug("No endpoint reference found for driver: {}, skipping close", driverId);
        }
    }

    public void closeContainerConnection(String containerId) {
        ContainerEndpointRef endpointRef = connectContainer(containerId);
        if (endpointRef != null) {
            endpointRef.closeEndpoint();
        } else {
            LOGGER.debug("No endpoint reference found for container: {}, skipping close", containerId);
        }
    }

    private MasterEndpointRef connectMaster(String masterId) {
        ResourceData resourceData = getResourceData(masterId);
        if (resourceData == null) {
            LOGGER.warn("Resource data not found for master: {}, skipping connection", masterId);
            return null;
        }
        return refFactory.connectMaster(resourceData.getHost(), resourceData.getRpcPort());
    }

    private ResourceManagerEndpointRef connectRM(String masterId) {
        ResourceData resourceData = getResourceData(masterId);
        return refFactory.connectResourceManager(resourceData.getHost(), resourceData.getRpcPort());
    }

    private DriverEndpointRef connectDriver(String driverId) {
        ResourceData resourceData = getResourceData(driverId);
        if (resourceData == null) {
            LOGGER.warn("Resource data not found for driver: {}, skipping connection", driverId);
            return null;
        }
        return refFactory.connectDriver(resourceData.getHost(), resourceData.getRpcPort());
    }

    private ContainerEndpointRef connectContainer(String containerId) {
        ResourceData resourceData = getResourceData(containerId);
        if (resourceData == null) {
            LOGGER.warn("Resource data not found for container: {}, skipping connection", containerId);
            return null;
        }
        return refFactory.connectContainer(resourceData.getHost(), resourceData.getRpcPort());
    }

    private PipelineMasterEndpointRef connectPipelineManager(String id) {
        ResourceData resourceData = getResourceData(id);
        return refFactory.connectPipelineManager(resourceData.getHost(), resourceData.getRpcPort());
    }

    private MetricEndpointRef connectMetricServer(String id) {
        ResourceData resourceData = getResourceData(id);
        return refFactory.connectMetricServer(resourceData.getHost(), resourceData.getMetricPort());
    }

    private SupervisorEndpointRef connectSupervisor(String id) {
        ResourceData resourceData = loadSupervisorData(id, true);
        return connectSupervisor(resourceData);
    }

    private SupervisorEndpointRef connectSupervisor(ResourceData resourceData) {
        return refFactory.connectSupervisor(resourceData.getHost(),
            resourceData.getSupervisorPort());
    }

    private ResourceData loadSupervisorData(String id, boolean fastFailure) {
        ResourceData resourceData;
        if (fastFailure) {
            resourceData = haService.loadResource(id);
        } else {
            resourceData = ((AbstractHAService) haService).loadDataFromStore(id,
                true, ResourceData::getSupervisorPort);
        }
        return resourceData;
    }

    private <T> T doRpcWithRetry(Callable<T> function, String resourceId,
                                 EndpointType endpointType) {
        return doRpcWithRetry(function, resourceId, endpointType, retryTimes);
    }

    private <T> T doRpcWithRetry(Callable<T> function, String resourceId, EndpointType endpointType,
                                 int retryTimes) {
        return RetryCommand.run(() -> {
            try {
                return function.call();
            } catch (Throwable t) {
                throw handleRpcException(resourceId, endpointType, t);
            }
        }, retryTimes, retryIntervalMs);
    }

    private void doRpcWithRetry(Runnable function, String resourceId, EndpointType endpointType) {
        RetryCommand.run(() -> {
            try {
                function.run();
            } catch (Throwable t) {
                throw handleRpcException(resourceId, endpointType, t);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    private Exception handleRpcException(String resourceId, EndpointType endpointType,
                                         Throwable t) {
        try {
            invalidateEndpointCache(resourceId, endpointType);
        } catch (Throwable e) {
            LOGGER.warn("invalidate rpc cache {} failed: {}", resourceId, e);
        }
        return new GeaflowRuntimeException(String.format("do rpc failed. %s", t.getMessage()), t);
    }

    protected void invalidateEndpointCache(String resourceId, EndpointType endpointType) {
        ResourceData resourceData = haService.invalidateResource(resourceId);
        if (resourceData != null) {
            refFactory.invalidateEndpointCache(resourceData.getHost(), resourceData.getRpcPort(),
                endpointType);
        }
    }

    protected ResourceData getResourceData(String resourceId) {
        if (haService == null) {
            LOGGER.warn("HAService is not initialized, cannot resolve resource: {}", resourceId);
            return null;
        }
        ResourceData resourceData = haService.resolveResource(resourceId);
        if (resourceData == null) {
            LOGGER.warn("Resource data not found for resource: {}", resourceId);
        }
        return resourceData;
    }

    public ExecutorService getExecutor() {
        return executorService;
    }
}
