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
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.geaflow.cluster.rpc.IAsyncSupervisorEndpoint;
import org.apache.geaflow.cluster.rpc.ISupervisorEndpointRef;
import org.apache.geaflow.cluster.rpc.RpcUtil;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.rpc.proto.Supervisor.RestartRequest;
import org.apache.geaflow.rpc.proto.Supervisor.StatusResponse;

public class SupervisorEndpointRef extends AbstractRpcEndpointRef implements ISupervisorEndpointRef {

    private IAsyncSupervisorEndpoint supervisorEndpoint;
    private final Empty empty;

    public SupervisorEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
        this.empty = Empty.newBuilder().build();
    }

    @Override
    protected void getRpcEndpoint() {
        this.supervisorEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncSupervisorEndpoint.class);
    }

    @Override
    protected RpcClientOptions getClientOptions() {
        RpcClientOptions options = super.getClientOptions();
        options.setGlobalThreadPoolSharing(false);
        options.setMaxTotalConnections(2);
        options.setMinIdleConnections(2);
        options.setIoThreadNum(1);
        options.setWorkThreadNum(2);
        options.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
        return options;
    }

    @Override
    public Future<Empty> restart(int pid, RpcCallback<Empty> callback) {
        CompletableFuture<Empty> result = new CompletableFuture<>();
        com.baidu.brpc.client.RpcCallback<Empty> rpcCallback =
            RpcUtil.buildRpcCallback(callback, result);
        RestartRequest request = RestartRequest.newBuilder().setPid(pid).build();
        supervisorEndpoint.restart(request, rpcCallback);
        return result;
    }

    @Override
    public StatusResponse status() {
        return supervisorEndpoint.status(empty);
    }

}
