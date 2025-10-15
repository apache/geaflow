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

package org.apache.geaflow.metaserver;

import static org.apache.geaflow.metaserver.Constants.APP_NAME;
import static org.apache.geaflow.metaserver.Constants.META_SERVER;

import com.baidu.brpc.server.RpcServer;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.metaserver.api.NamespaceServiceHandler;
import org.apache.geaflow.metaserver.api.ServiceHandlerFactory;
import org.apache.geaflow.metaserver.model.protocal.MetaRequest;
import org.apache.geaflow.metaserver.model.protocal.MetaResponse;
import org.apache.geaflow.metaserver.model.protocal.request.RequestPBConverter;
import org.apache.geaflow.metaserver.model.protocal.response.ResponsePBConverter;
import org.apache.geaflow.metaserver.service.MetaServerService;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import org.apache.geaflow.service.discovery.ServiceBuilder;
import org.apache.geaflow.service.discovery.ServiceBuilderFactory;
import org.apache.geaflow.service.discovery.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaServer implements MetaServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaServer.class);
    private static final int MIN_PORT = 50000;
    private static final int MAX_PORT = 60000;
    private RpcServer rpcServer;
    private Map<NamespaceType, NamespaceServiceHandler> namespaceServiceHandlerMap;
    private ServiceProvider serviceProvider;

    public void init(MetaServerContext context) {
        Configuration configuration = context.getConfiguration();
        ServiceBuilder serviceBuilder = ServiceBuilderFactory.build(configuration.getString(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE));
        serviceProvider = serviceBuilder.buildProvider(configuration);
        namespaceServiceHandlerMap = ServiceHandlerFactory.load(context);
        startServer(configuration);
    }

    private void startServer(Configuration configuration) {
        int port = PortUtil.getPort(MIN_PORT, MAX_PORT);
        rpcServer = new RpcServer(port);
        rpcServer.registerService(new MetaServerServiceProxy(this));
        rpcServer.start();

        HostAndPort info = new HostAndPort(ProcessUtil.getHostIp(), port);

        String appName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        serviceProvider.update(APP_NAME, appName.getBytes());

        if (serviceProvider.exists(META_SERVER)) {
            serviceProvider.delete(META_SERVER);
        }

        serviceProvider.createAndWatch(META_SERVER,
            SerializerFactory.getKryoSerializer().serialize(info));
        LOGGER.info("{} meta server start at {}", appName, info);
    }

    @Override
    public ServiceResultPb process(ServiceRequestPb serviceRequest) {
        MetaRequest request = RequestPBConverter.convert(serviceRequest);
        NamespaceServiceHandler handler = namespaceServiceHandlerMap.get(request.namespaceType());
        MetaResponse response = handler.process(request);
        return ResponsePBConverter.convert(response);
    }

    public void close() {
        rpcServer.shutdown();
        serviceProvider.close();
    }

}
