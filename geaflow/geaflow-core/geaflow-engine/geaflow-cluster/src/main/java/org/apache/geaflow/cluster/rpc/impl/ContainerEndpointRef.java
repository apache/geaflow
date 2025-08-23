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
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.rpc.IAsyncContainerEndpoint;
import org.apache.geaflow.cluster.rpc.IContainerEndpointRef;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.rpc.proto.Container;
import org.apache.geaflow.rpc.proto.Container.Request;
import org.apache.geaflow.rpc.proto.Container.Response;

public class ContainerEndpointRef extends AbstractRpcEndpointRef implements IContainerEndpointRef {

    protected IAsyncContainerEndpoint containerEndpoint;

    public ContainerEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.containerEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncContainerEndpoint.class);
    }

    @Override
    public Future<IEvent> process(IEvent request, RpcCallback<Response> callback) {
        CompletableFuture<IEvent> result = new CompletableFuture<>();
        Container.Request req = buildRequest(request);
        this.containerEndpoint.process(req, new com.baidu.brpc.client.RpcCallback<Response>() {
            @Override
            public void success(Response response) {
                if (callback != null) {
                    callback.onSuccess(response);
                }
                ByteString payload = response.getPayload();
                IEvent event;
                if (payload == ByteString.EMPTY) {
                    event = null;
                } else {
                    event = RpcMessageEncoder.decode(payload);
                }
                result.complete(event);
            }

            @Override
            public void fail(Throwable throwable) {
                callback.onFailure(throwable);
                result.completeExceptionally(throwable);
            }
        });
        return result;
    }

    @Override
    public void closeEndpoint() {
        this.containerEndpoint.close(Empty.newBuilder().build());
        super.closeEndpoint();
    }

    protected Request buildRequest(IEvent request) {
        ByteString payload = RpcMessageEncoder.encode(request);
        return Request.newBuilder().setPayload(payload).build();
    }

}
