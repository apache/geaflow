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

package org.apache.geaflow.cluster.web.metrics;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.HTTP_REST_SERVICE_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_SERVICE_PORT;

import com.baidu.brpc.server.RpcServerOptions;
import java.io.Serializable;
import org.apache.geaflow.cluster.rpc.RpcService;
import org.apache.geaflow.cluster.rpc.impl.MetricEndpoint;
import org.apache.geaflow.cluster.rpc.impl.RpcServiceImpl;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.rpc.ConfigurableServerOption;
import org.apache.geaflow.common.utils.PortUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricServer implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricServer.class);

    private final int port;
    private RpcService rpcService;

    public MetricServer(Configuration configuration) {
        this.port = configuration.getInteger(METRIC_SERVICE_PORT);
        if (configuration.getBoolean(HTTP_REST_SERVICE_ENABLE)) {
            RpcServerOptions serverOptions = getServerOptions(configuration);
            RpcServiceImpl rpcService = new RpcServiceImpl(PortUtil.getPort(port), serverOptions);
            rpcService.addEndpoint(new MetricEndpoint(configuration));
            this.rpcService = rpcService;
        }
    }

    private RpcServerOptions getServerOptions(Configuration configuration) {
        RpcServerOptions serverOptions = ConfigurableServerOption.build(configuration);
        serverOptions.setGlobalThreadPoolSharing(false);
        serverOptions.setIoThreadNum(1);
        serverOptions.setWorkThreadNum(2);
        return serverOptions;
    }

    public int start() {
        if (rpcService != null) {
            int metricPort = rpcService.startService();
            LOGGER.info("started metric service on port:{}", metricPort);
            return metricPort;
        } else {
            return port;
        }
    }

    public void stop() {
        if (rpcService != null) {
            LOGGER.info("stopping metric query service");
            rpcService.stopService();
        }
    }

}
