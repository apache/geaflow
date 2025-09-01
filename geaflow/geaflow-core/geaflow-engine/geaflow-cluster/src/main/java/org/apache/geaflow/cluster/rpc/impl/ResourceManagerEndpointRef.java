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

package org.apache.geaflow.cluster.rpc.impl;

import com.baidu.brpc.client.BrpcProxy;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.ReleaseResponse;
import org.apache.geaflow.cluster.resourcemanager.RequireResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.RequireResponse;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.rpc.IResourceEndpointRef;
import org.apache.geaflow.cluster.rpc.IResourceManagerEndpoint;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.rpc.proto.Resource;

public class ResourceManagerEndpointRef extends AbstractRpcEndpointRef implements IResourceEndpointRef {

    private IResourceManagerEndpoint resourceManagerEndpoint;

    public ResourceManagerEndpointRef(String host, int port,
                                      Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.resourceManagerEndpoint = BrpcProxy.getProxy(rpcClient, IResourceManagerEndpoint.class);
    }

    @Override
    public RequireResponse requireResource(RequireResourceRequest request) {
        Resource.RequireResourceResponse response = this.resourceManagerEndpoint.requireResource(convertRequireRequest(request));
        return convertRequireResponse(response);
    }

    @Override
    public ReleaseResponse releaseResource(ReleaseResourceRequest request) {
        Resource.ReleaseResourceResponse response = this.resourceManagerEndpoint.releaseResource(convertReleaseRequest(request));
        return convertReleaseResponse(response);
    }

    private static Resource.RequireResourceRequest convertRequireRequest(RequireResourceRequest request) {
        Resource.AllocateStrategy strategy;
        switch (request.getAllocateStrategy()) {
            case ROUND_ROBIN:
                strategy = Resource.AllocateStrategy.ROUND_ROBIN;
                break;
            case PROCESS_FAIR:
                strategy = Resource.AllocateStrategy.PROCESS_FAIR;
                break;
            default:
                String msg = "unrecognized allocate strategy" + request.getAllocateStrategy();
                throw new GeaflowRuntimeException(RuntimeErrors.INST.resourceError(msg));
        }
        return Resource.RequireResourceRequest.newBuilder()
            .setRequireId(request.getRequireId())
            .setWorkersNum(request.getRequiredNum())
            .setAllocStrategy(strategy).build();
    }

    private static RequireResponse convertRequireResponse(Resource.RequireResourceResponse response) {
        String requireId = response.getRequireId();
        boolean success = response.getSuccess();
        String msg = response.getMsg();
        if (!success) {
            return RequireResponse.fail(requireId, msg);
        }
        List<WorkerInfo> workers = response.getWorkerList().stream()
            .map(w -> WorkerInfo.build(w.getHost(), w.getRpcPort(),
                w.getShufflePort(), w.getProcessId(), w.getProcessIndex(), w.getWorkerId(), w.getContainerId()))
            .collect(Collectors.toList());
        return RequireResponse.success(requireId, workers);
    }

    private static Resource.ReleaseResourceRequest convertReleaseRequest(ReleaseResourceRequest request) {
        Resource.ReleaseResourceRequest.Builder builder = Resource.ReleaseResourceRequest.newBuilder();
        builder.setReleaseId(request.getReleaseId());
        for (WorkerInfo workerInfo : request.getWorkers()) {
            Resource.Worker worker = Resource.Worker.newBuilder()
                .setHost(workerInfo.getHost())
                .setProcessId(workerInfo.getProcessId())
                .setProcessIndex(workerInfo.getProcessIndex())
                .setRpcPort(workerInfo.getRpcPort())
                .setWorkerId(workerInfo.getWorkerIndex())
                .setContainerId(workerInfo.getContainerName())
                .build();
            builder.addWorker(worker);
        }
        return builder.build();
    }

    private static ReleaseResponse convertReleaseResponse(Resource.ReleaseResourceResponse response) {
        String releaseId = response.getReleaseId();
        return response.getSuccess()
            ? ReleaseResponse.success(releaseId)
            : ReleaseResponse.fail(releaseId, response.getMsg());
    }

}
