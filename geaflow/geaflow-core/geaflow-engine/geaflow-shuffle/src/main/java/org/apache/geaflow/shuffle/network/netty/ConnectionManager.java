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

package org.apache.geaflow.shuffle.network.netty;

import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.network.ConnectionId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.network.ITransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager implements IConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final ShuffleConfig nettyConfig;
    private final ShuffleAddress shuffleAddress;
    private final SliceRequestClientFactory clientFactory;
    private final ExecutorService executor;

    private NettyServer server;
    private NettyClient client;

    public ConnectionManager(ShuffleConfig config) {
        ITransportContext context = new NettyContext(config);
        this.client = new NettyClient(config, context);
        this.server = new NettyServer(config, context);
        InetSocketAddress address = server.start();
        this.shuffleAddress = new ShuffleAddress(address.getAddress().getHostAddress(),
            address.getPort());
        this.clientFactory = new SliceRequestClientFactory(config, client);
        this.nettyConfig = config;
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), ThreadUtil.namedThreadFactory(true, "connect"));
    }

    public ShuffleAddress getShuffleAddress() {
        return shuffleAddress;
    }

    public ShuffleConfig getShuffleConfig() {
        return nettyConfig;
    }

    public PooledByteBufAllocator getServerBufAllocator() {
        return server.getPooledAllocator();
    }

    public PooledByteBufAllocator getClientBufAllocator() {
        return client.getAllocator();
    }

    public NettyClient getClient() {
        return client;
    }

    public SliceRequestClient createSliceRequestClient(ConnectionId connectionId)
        throws IOException, InterruptedException {
        return clientFactory.createSliceRequestClient(connectionId);
    }

    public void closeOpenChannelConnections(ConnectionId connectionId) {
        clientFactory.closeOpenChannelConnections(connectionId);
    }

    public void close() throws IOException {
        LOGGER.info("closing connection manager");
        if (server != null) {
            server.close();
            server = null;
        }
        if (client != null) {
            client.shutdown();
            client = null;
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

}
