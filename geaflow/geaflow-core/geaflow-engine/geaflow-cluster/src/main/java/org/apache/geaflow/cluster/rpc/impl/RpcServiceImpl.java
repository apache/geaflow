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

import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import java.io.Serializable;
import org.apache.geaflow.cluster.rpc.RpcService;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcServiceImpl implements RpcService, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServiceImpl.class);

    private final int port;

    private final RpcServer server;

    public RpcServiceImpl(int port, RpcServerOptions options) {
        this.port = port;
        this.server = new RpcServer(port, options);
    }

    public void addEndpoint(Object rpcEndpoint) {
        server.registerService(rpcEndpoint);
    }

    @Override
    public int startService() {
        try {
            this.server.start();
            LOGGER.info("Brpc Server started: {}", port);
            return port;
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw new GeaflowRuntimeException(t);
        }
    }

    @Test
    public void waitTermination() {
        synchronized (server) {
            while (!server.isShutdown()) {
                try {
                    server.wait();
                } catch (InterruptedException e) {
                    LOGGER.warn("shutdown is interrupted");
                }
            }
        }
    }

    @Override
    public void stopService() {
        if (server != null) {
            server.shutdown();
        }
    }
}
