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

import io.netty.channel.Channel;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.network.ConnectionId;
import org.apache.geaflow.shuffle.util.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceRequestClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceRequestClientFactory.class);

    private final int retryNumber;
    private final NettyClient nettyClient;

    private final ConcurrentMap<ConnectionId, CompletableFuture<SliceRequestClient>> clients =
        new ConcurrentHashMap<>();

    public SliceRequestClientFactory(ShuffleConfig nettyConfig, NettyClient nettyClient) {
        this.nettyClient = nettyClient;
        this.retryNumber = nettyConfig.getConnectMaxRetries();
    }

    /**
     * Atomically establishes a TCP connection to the given remote address and
     * creates a {@link SliceRequestClient} instance for this connection.
     */
    public SliceRequestClient createSliceRequestClient(ConnectionId connectionId)
        throws InterruptedException {
        while (true) {
            final CompletableFuture<SliceRequestClient> newClientFuture =
                new CompletableFuture<>();

            CompletableFuture<SliceRequestClient> clientFuture = clients.putIfAbsent(connectionId
                , newClientFuture);

            final SliceRequestClient client;

            if (clientFuture == null) {
                try {
                    client = connectWithRetries(connectionId);
                } catch (Throwable e) {
                    newClientFuture.completeExceptionally(
                        new IOException("Could not create client.", e));
                    clients.remove(connectionId, newClientFuture);
                    throw e;
                }
                newClientFuture.complete(client);
            } else {
                try {
                    client = clientFuture.get();
                } catch (ExecutionException e) {
                    throw new GeaflowRuntimeException("connect failed", e);
                }
            }

            // Make sure to increment the reference count before handing a client
            // out to ensure correct bookkeeping for channel closing.
            if (client.incrementReferenceCounter()) {
                return client;
            } else {
                destroyRequestClient(connectionId, client);
            }
        }
    }

    private SliceRequestClient connectWithRetries(ConnectionId connectionId) {
        int tried = 0;
        long startTime = System.nanoTime();
        while (true) {
            try {
                SliceRequestClient client = connect(connectionId);
                LOGGER.info("Successfully created connection to {} after {} ms", connectionId,
                    (System.nanoTime() - startTime) / 1000000);
                return client;
            } catch (TransportException e) {
                tried++;
                LOGGER.error("failed to connect to {}, retry #{}", connectionId, tried, e);
                if (tried > retryNumber) {
                    throw new GeaflowRuntimeException(String.format("Failed to connect to %s",
                        connectionId.getAddress()), e);
                }
            }
        }
    }

    private SliceRequestClient connect(ConnectionId connectionId) throws TransportException {
        try {
            Channel channel = nettyClient.connect(connectionId.getAddress()).await().channel();
            SliceRequestClientHandler clientHandler = channel.pipeline()
                .get(SliceRequestClientHandler.class);
            return new SliceRequestClient(channel, clientHandler, connectionId, this);
        } catch (Exception e) {
            throw new TransportException(
                "Connecting to remote server '" + connectionId.getAddress()
                    + "' has failed. This might indicate that the remote server has been lost.",
                connectionId.getAddress(), e);
        }
    }

    public void closeOpenChannelConnections(ConnectionId connectionId) {
        CompletableFuture<SliceRequestClient> entry = clients.get(connectionId);

        if (entry != null && !entry.isDone()) {
            entry.thenAccept(client -> {
                if (client.disposeIfNotUsed()) {
                    clients.remove(connectionId, entry);
                }
            });
        }
    }

    /**
     * Removes the client for the given {@link ConnectionId}.
     */
    public void destroyRequestClient(ConnectionId connectionId,
                                     SliceRequestClient client) {
        final CompletableFuture<SliceRequestClient> future = clients.get(connectionId);
        if (future != null && future.isDone()) {
            future.thenAccept(futureClient -> {
                if (client.equals(futureClient)) {
                    clients.remove(connectionId, future);
                }
            });
        }
    }

}
