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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.geaflow.cluster.rpc.IAsyncMetricEndpoint;
import org.apache.geaflow.cluster.rpc.IMetricEndpointRef;
import org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory;
import org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointRefID;
import org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType;
import org.apache.geaflow.cluster.rpc.RpcUtil;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import org.apache.geaflow.rpc.proto.Metrics.MetricQueryResponse;

public class MetricEndpointRef extends AbstractRpcEndpointRef implements IMetricEndpointRef {

    private IAsyncMetricEndpoint metricEndpoint;
    private final EndpointRefID refID;

    public MetricEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
        this.refID = new EndpointRefID(host, port, EndpointType.METRIC);
    }

    @Override
    protected void getRpcEndpoint() {
        this.metricEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncMetricEndpoint.class);
    }

    @Override
    protected RpcClientOptions getClientOptions() {
        RpcClientOptions options = super.getClientOptions();
        options.setGlobalThreadPoolSharing(false);
        options.setMaxTotalConnections(2);
        options.setMinIdleConnections(1);
        options.setIoThreadNum(1);
        options.setWorkThreadNum(1);
        options.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
        return options;
    }

    @Override
    public Future<MetricQueryResponse> queryMetrics(MetricQueryRequest request, RpcCallback<MetricQueryResponse> callback) {
        CompletableFuture<MetricQueryResponse> result = new CompletableFuture<>();
        com.baidu.brpc.client.RpcCallback<MetricQueryResponse> rpcCallback =
            RpcUtil.buildRpcCallback(callback, result);
        try {
            this.metricEndpoint.queryMetrics(request, rpcCallback);
        } catch (Throwable e) {
            rpcCallback.fail(e);
            RpcEndpointRefFactory.getInstance().invalidateRef(refID);
        }
        return result;
    }

}
