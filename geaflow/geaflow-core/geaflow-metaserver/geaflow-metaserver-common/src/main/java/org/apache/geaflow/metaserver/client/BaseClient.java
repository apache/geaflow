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

package org.apache.geaflow.metaserver.client;

import static org.apache.geaflow.metaserver.Constants.META_SERVER;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.rpc.ConfigurableClientOption;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.metaserver.model.protocal.MetaRequest;
import org.apache.geaflow.metaserver.model.protocal.request.RequestPBConverter;
import org.apache.geaflow.metaserver.model.protocal.response.ResponsePBConverter;
import org.apache.geaflow.metaserver.service.MetaServerService;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import org.apache.geaflow.service.discovery.ServiceBuilder;
import org.apache.geaflow.service.discovery.ServiceBuilderFactory;
import org.apache.geaflow.service.discovery.ServiceConsumer;
import org.apache.geaflow.service.discovery.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseClient implements ServiceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);
    private static final int MAX_RETRY = 5;
    private static final int RETRY_MS = 10000;
    protected ServiceConsumer serviceConsumer;
    protected RpcClient rpcClient;
    protected MetaServerService metaServerService;
    private HostAndPort currentServiceInfo;
    protected Configuration configuration;

    public BaseClient() {
    }

    public BaseClient(Configuration configuration) {
        this.configuration = configuration;
        ServiceBuilder serviceBuilder = ServiceBuilderFactory.build(
            configuration.getString(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE));
        serviceConsumer = serviceBuilder.buildConsumer(configuration);
        serviceConsumer.register(this);
        buildServerConnect(true);
    }

    protected synchronized void buildServerConnect(boolean force) {
        boolean exits = serviceConsumer.exists(META_SERVER);
        if (!exits) {
            if (!force) {
                return;
            }
            throw new IllegalStateException("not find meta server info");
        }
        byte[] bytes = serviceConsumer.getDataAndWatch(META_SERVER);
        HostAndPort serviceInfo = (HostAndPort) SerializerFactory.getKryoSerializer()
            .deserialize(bytes);
        if (currentServiceInfo != null && currentServiceInfo.equals(serviceInfo)) {
            LOGGER.info("service info {} is same, skip update", currentServiceInfo);
            return;
        }

        if (rpcClient != null) {
            rpcClient.stop();
        }

        LOGGER.info("connect to meta server {}", serviceInfo);

        rpcClient = new RpcClient(new Endpoint(serviceInfo.getHost(), serviceInfo.getPort()),
            ConfigurableClientOption.build(configuration));
        metaServerService = BrpcProxy.getProxy(rpcClient, MetaServerService.class);
        currentServiceInfo = serviceInfo;
    }

    protected <T> T process(MetaRequest request) {
        return (T) RetryCommand.run(
            () -> {
                ServiceResultPb result = metaServerService.process(RequestPBConverter.convert(request));
                return ResponsePBConverter.convert(result);
            },
            () -> {
                buildServerConnect(false);
                return true;
            },
            MAX_RETRY, RETRY_MS, true);
    }

    @Override
    public void nodeCreated(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeDeleted(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeDataChanged(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeChildrenChanged(String path) {
        checkAndUpdateConnector(path);
    }

    private void checkAndUpdateConnector(String path) {
        if (path.contains(META_SERVER)) {
            buildServerConnect(false);
        }
    }

    public void close() {
        serviceConsumer.close();
        rpcClient.stop();
    }
}
