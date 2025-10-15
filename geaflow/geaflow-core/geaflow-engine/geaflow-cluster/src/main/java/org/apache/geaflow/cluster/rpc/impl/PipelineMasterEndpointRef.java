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
import java.util.concurrent.Future;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.rpc.IPipelineManagerEndpointRef;
import org.apache.geaflow.cluster.rpc.IPipelineMasterEndpoint;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.rpc.proto.Container;
import org.apache.geaflow.rpc.proto.Container.Request;
import org.apache.geaflow.rpc.proto.Container.Response;

public class PipelineMasterEndpointRef extends AbstractRpcEndpointRef implements IPipelineManagerEndpointRef {

    protected IPipelineMasterEndpoint pipelineMasterEndpoint;

    public PipelineMasterEndpointRef(String host, int port,
                                     Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.pipelineMasterEndpoint = BrpcProxy.getProxy(rpcClient, IPipelineMasterEndpoint.class);
    }

    @Override
    public Future<IEvent> process(IEvent request, RpcCallback<Response> callback) {
        Container.Request taskEvent = buildRequest(request);
        this.pipelineMasterEndpoint.process(taskEvent);
        return null;
    }

    protected Request buildRequest(IEvent request) {
        ByteString payload = RpcMessageEncoder.encode(request);
        return Request.newBuilder().setPayload(payload).build();
    }
}
