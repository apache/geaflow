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

import static com.google.common.base.Preconditions.checkState;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.network.ITransportContext;
import org.apache.geaflow.shuffle.network.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.netty.NettyClient.
 */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
    private static final String CLIENT_THREAD_GROUP_NAME = "NettyClient";

    private final ShuffleConfig config;
    private final PooledByteBufAllocator allocator;
    private Bootstrap bootstrap;

    public NettyClient(ShuffleConfig config, ITransportContext context) {
        this.config = config;
        this.bootstrap = new Bootstrap();

        final long start = System.nanoTime();

        // --------------------------------------------------------------------
        // Transport-specific configuration
        // --------------------------------------------------------------------
        if (Epoll.isAvailable()) {
            initEpollBootstrap();
            LOGGER.info("Transport type 'auto': using EPOLL.");
        } else {
            initNioBootstrap();
            LOGGER.info("Transport type 'auto': using NIO.");
        }

        // --------------------------------------------------------------------
        // Configuration
        // --------------------------------------------------------------------

        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

        // Timeout for new connections.
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMs());

        // Pooled allocator for Netty's ByteBuf instances.
        allocator = NettyUtils.createPooledByteBufAllocator(config.preferDirectBuffer(),
            config.isThreadCacheEnabled(), config.getClientNumThreads());
        bootstrap.option(ChannelOption.ALLOCATOR, allocator);

        // Receive and send buffer size.
        int sendBufferSize = config.getSendBufferSize();
        if (sendBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, sendBufferSize);
        }
        int receiveBufferSize = config.getReceiveBufferSize();
        if (receiveBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, receiveBufferSize);
        }

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline().addLast(context.createClientChannelHandlers(channel));
            }
        });

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOGGER.info("Successful initialization (took {} ms).", duration);
    }

    private void initNioBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.

        NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getClientNumThreads(),
            NettyUtils.getNamedThreadFactory(CLIENT_THREAD_GROUP_NAME));
        bootstrap.group(nioGroup).channel(NioSocketChannel.class);
    }

    private void initEpollBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.

        EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getClientNumThreads(),
            NettyUtils.getNamedThreadFactory(CLIENT_THREAD_GROUP_NAME));
        bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
    }

    // ------------------------------------------------------------------------
    // Client connections
    // ------------------------------------------------------------------------
    public ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
        checkState(null != bootstrap, "Client has not been initialized yet.");

        try {
            return bootstrap.connect(serverSocketAddress);
        } catch (ChannelException e) {
            final String message = "Too many open files";
            if ((e.getCause() instanceof java.net.SocketException && message
                .equals(e.getCause().getMessage())) || (e.getCause() instanceof ChannelException
                && e.getCause().getCause() instanceof java.net.SocketException && message
                .equals(e.getCause().getCause().getMessage()))) {
                throw new GeaflowRuntimeException(
                    "The operating system does not offer enough file handles to open the network "
                        + "connection. Please increase the number of available file handles.",
                    e.getCause());
            } else {
                throw e;
            }
        }
    }

    public ShuffleConfig getConfig() {
        return config;
    }

    public PooledByteBufAllocator getAllocator() {
        return allocator;
    }

    public void shutdown() {
        final long start = System.nanoTime();

        if (bootstrap != null) {
            if (bootstrap.group() != null) {
                bootstrap.group().shutdownGracefully();
            }
            bootstrap = null;
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOGGER.info("Successful shutdown (took {} ms).", duration);
    }

}
