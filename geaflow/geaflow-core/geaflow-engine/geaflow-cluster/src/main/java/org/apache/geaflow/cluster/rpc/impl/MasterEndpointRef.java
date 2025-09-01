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
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.geaflow.cluster.driver.DriverInfo;
import org.apache.geaflow.cluster.rpc.IAsyncMasterEndpoint;
import org.apache.geaflow.cluster.rpc.IMasterEndpointRef;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef;
import org.apache.geaflow.cluster.rpc.RpcUtil;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.apache.geaflow.rpc.proto.Master.HeartbeatRequest;
import org.apache.geaflow.rpc.proto.Master.HeartbeatResponse;
import org.apache.geaflow.rpc.proto.Master.RegisterRequest;
import org.apache.geaflow.rpc.proto.Master.RegisterResponse;

public class MasterEndpointRef extends AbstractRpcEndpointRef implements IMasterEndpointRef {

    private IAsyncMasterEndpoint masterEndpoint;

    public MasterEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.masterEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncMasterEndpoint.class);
    }

    public <T> Future<RegisterResponse> registerContainer(T info, RpcEndpointRef.RpcCallback<RegisterResponse> callback) {
        CompletableFuture<RegisterResponse> result = new CompletableFuture<>();
        ByteString payload = RpcMessageEncoder.encode(info);
        RegisterRequest register = RegisterRequest.newBuilder().setPayload(payload).build();
        com.baidu.brpc.client.RpcCallback<RegisterResponse> rpcCallback = RpcUtil.buildRpcCallback(callback, result);
        if (info instanceof DriverInfo) {
            this.masterEndpoint.registerDriver(register, rpcCallback);
        } else {
            this.masterEndpoint.registerContainer(register, rpcCallback);
        }
        return result;
    }

    @Override
    public Future<HeartbeatResponse> sendHeartBeat(Heartbeat heartbeat, RpcCallback<HeartbeatResponse> callback) {
        CompletableFuture<HeartbeatResponse> result = new CompletableFuture<>();
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(heartbeat.getContainerId())
            .setTimestamp(heartbeat.getTimestamp())
            .setName(RpcMessageEncoder.encode(heartbeat.getContainerName()))
            .setPayload(RpcMessageEncoder.encode(heartbeat.getProcessMetrics()))
            .build();
        com.baidu.brpc.client.RpcCallback<HeartbeatResponse> rpcCallback = RpcUtil.buildRpcCallback(callback, result);
        this.masterEndpoint.receiveHeartbeat(heartbeatRequest, rpcCallback);
        return result;
    }

    @Override
    public Empty sendException(Integer containerId, String containerName, String message) {
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
            .setId(containerId)
            .setName(RpcMessageEncoder.encode(containerName))
            .setPayload(RpcMessageEncoder.encode(message))
            .build();
        return masterEndpoint.receiveException(heartbeatRequest);
    }

    @Override
    public void closeEndpoint() {
        this.masterEndpoint.close(Empty.newBuilder().build());
        super.closeEndpoint();
    }
}
