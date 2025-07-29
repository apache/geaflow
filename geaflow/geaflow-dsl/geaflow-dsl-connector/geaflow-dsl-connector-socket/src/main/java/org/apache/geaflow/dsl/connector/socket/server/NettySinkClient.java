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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettySinkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettySinkClient.class.getName());

    private final String host;

    private final int port;

    private final LinkedBlockingQueue<String> dataQueue;

    public NettySinkClient(String host, int port, LinkedBlockingQueue<String> dataQueue) {
        this.host = host;
        this.port = port;
        this.dataQueue = dataQueue;
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap().group(bossGroup).channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) throws Exception {
                    channel.pipeline().addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE));
                    channel.pipeline().addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
                    channel.pipeline().addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));
                    channel.pipeline().addLast(new ClientHandler());
                }
            });

        ChannelFuture future = bootstrap.connect(host, port).sync();
        future.channel().closeFuture().addListener((channelFuture) -> bossGroup.shutdownGracefully());

    }

    private class ClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().eventLoop().submit(new SendDataTask(ctx));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            String data = String.valueOf(msg);
            LOGGER.info("sink receive data: {}", data);
        }
    }

    private class SendDataTask implements Runnable {

        private final ChannelHandlerContext context;

        public SendDataTask(ChannelHandlerContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String data = dataQueue.take();
                    context.writeAndFlush(data + "\n");
                } catch (InterruptedException e) {
                    LOGGER.info("Send data error. ", e);
                }
            }
        }
    }
}
