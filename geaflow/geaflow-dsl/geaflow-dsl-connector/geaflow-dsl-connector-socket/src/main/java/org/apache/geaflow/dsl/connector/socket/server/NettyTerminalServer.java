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

package org.apache.geaflow.dsl.connector.socket.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyTerminalServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTerminalServer.class.getName());

    private PrintStream printer;

    private LinkedBlockingQueue<String> dataQueue = new LinkedBlockingQueue<>();

    public void bind(int port) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        printer = System.out;

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new NettyServerChannelInitializer())
                .option(ChannelOption.SO_BACKLOG, 500)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            printer.println("Start netty terminal server, waiting connect.");
            // bind port
            ChannelFuture future = bootstrap.bind(port).sync();
            // shutdown channel
            future.channel().closeFuture().addListener((channelFuture) -> bossGroup.shutdownGracefully());
            Scanner scanner = new Scanner(System.in);
            while (true) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    dataQueue.put(line);
                }
            }
        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private class NettyServerChannelInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            channel.pipeline().addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE));
            channel.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8));
            channel.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8));
            channel.pipeline().addLast("commonHandler", new SocketHandler());
        }
    }

    private class SocketHandler extends ChannelInboundHandlerAdapter {

        private AtomicBoolean activeFlag = new AtomicBoolean(true);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            String message = String.valueOf(msg);
            if (message.startsWith(NettyWebServer.CMD_GET_DATA)) {
                printer.println("please enter the data to the console.");
                ctx.channel().eventLoop().scheduleWithFixedDelay(new GetDataTask(ctx, activeFlag)
                    , 0, 1, TimeUnit.SECONDS);
            } else {
                printer.println(">> " + message);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            activeFlag.set(false);
        }
    }

    private class GetDataTask implements Runnable {

        private final ChannelHandlerContext ctx;
        private final AtomicBoolean activeFlag;

        public GetDataTask(ChannelHandlerContext ctx, AtomicBoolean activeFlag) {
            this.ctx = ctx;
            this.activeFlag = activeFlag;
        }

        @Override
        public void run() {
            if (activeFlag.get()) {
                while (!dataQueue.isEmpty()) {
                    String line = null;
                    try {
                        line = dataQueue.take();
                        String res = line + "\n";
                        this.ctx.writeAndFlush(res);
                    } catch (InterruptedException e) {
                        LOGGER.info(e.getMessage());
                    }
                }
            }
        }
    }
}
