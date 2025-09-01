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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.lang3.SystemUtils;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.network.ITransportContext;
import org.apache.geaflow.shuffle.network.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.netty.NettyServer.
 */
public class NettyServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);
    private static final String SERVER_THREAD_GROUP_NAME = "NettyServer";

    private ServerBootstrap bootstrap;
    private ChannelFuture bindFuture;
    private ShuffleConfig config;
    private ITransportContext context;
    private PooledByteBufAllocator pooledAllocator;

    public NettyServer(ShuffleConfig config, ITransportContext transportContext) {
        this.config = config;
        this.context = transportContext;
    }

    public InetSocketAddress start() {
        checkState(bootstrap == null, "Netty server has already been initialized.");

        final long start = System.currentTimeMillis();
        bootstrap = new ServerBootstrap();

        if (Epoll.isAvailable()) {
            initEpollBootstrap(config);
        } else {
            initNioBootstrap(config);
        }

        bootstrap.option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS);
        // Pooled allocators for Netty's ByteBuf instances
        pooledAllocator = NettyUtils
            .createPooledByteBufAllocator(config.preferDirectBuffer(), config.isThreadCacheEnabled(),
                config.getServerThreadsNum());
        bootstrap.option(ChannelOption.ALLOCATOR, pooledAllocator);
        bootstrap.childOption(ChannelOption.ALLOCATOR, pooledAllocator);

        if (config.getServerConnectBacklog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
        }

        int receiveBufferSize = config.getReceiveBufferSize();
        if (receiveBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBufferSize);
        }
        int sendBufferSize = config.getSendBufferSize();
        if (sendBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, sendBufferSize);
        }

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline().addLast(context.createServerChannelHandler(channel));
                //context.initializePipeline(channel);
            }
        });

        // --------------------------------------------------------------------
        // Start Server
        // --------------------------------------------------------------------

        bootstrap.localAddress(config.getServerAddress(), config.getServerPort());
        bindFuture = bootstrap.bind().syncUninterruptibly();
        InetSocketAddress localAddress = (InetSocketAddress) bindFuture.channel().localAddress();

        long end = System.currentTimeMillis();
        LOGGER.info("Successful initialization (took {} ms). Listening on {}. NettyConfig: {}",
            (end - start), localAddress.toString(), config);

        return localAddress;
    }

    private void initNioBootstrap(ShuffleConfig config) {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name = String.format("%s(%s)", SERVER_THREAD_GROUP_NAME, config.getServerPort());

        NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getServerThreadsNum(),
            NettyUtils.getNamedThreadFactory(name));
        bootstrap.group(nioGroup).channel(NioServerSocketChannel.class);
    }

    private void initEpollBootstrap(ShuffleConfig config) {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name = String.format("%s(%s)", SERVER_THREAD_GROUP_NAME, config.getServerPort());

        EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getServerThreadsNum(),
            NettyUtils.getNamedThreadFactory(name));
        bootstrap.group(epollGroup).channel(EpollServerSocketChannel.class);
    }

    public PooledByteBufAllocator getPooledAllocator() {
        return pooledAllocator;
    }

    @Override
    public void close() throws IOException {
        final long start = System.currentTimeMillis();
        if (bindFuture != null) {
            bindFuture.channel().close().awaitUninterruptibly();
            bindFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        long end = System.currentTimeMillis();
        LOGGER.info("Successful shutdown (took {} ms).", (end - start));
    }

}
