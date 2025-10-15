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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyWebServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyWebServer.class.getName());

    public static final String CMD_GET_DATA = "GET_DATA";

    public static final String CMD_WEB_DISPLAY_DATA = "WEB_DISPLAY_DATA";

    private final LinkedBlockingQueue<String> inputDataQueue = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<String> outputDataQueue = new LinkedBlockingQueue<>();

    public void bind(int port) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new NettyServerChannelInitializer())
                .option(ChannelOption.SO_BACKLOG, 500)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            LOGGER.info("Start netty web server, waiting connect.");
            // bind port
            ChannelFuture future = bootstrap.bind(port).sync();
            // shutdown channel sync
            future.channel().closeFuture().sync();
        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private class NettyServerChannelInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            channel.pipeline().addLast("protocolChoose", new ProtocolChooseHandler());
        }
    }

    private class ProtocolChooseHandler extends ByteToMessageDecoder {

        private static final int MAX_LENGTH = 100;

        private static final String WEB_SOCKET_PREFIX = "GET /";

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            String protocol = getProtocol(in);
            if (protocol.startsWith(WEB_SOCKET_PREFIX)) {
                // add web socket protocol handler
                ctx.pipeline().addLast("httpCodec", new HttpServerCodec());
                ctx.pipeline().addLast("aggregator", new HttpObjectAggregator(65535));
                ctx.pipeline().addLast("webSocketAggregator", new WebSocketFrameAggregator(65535));
                ctx.pipeline().addLast("protocolHandler", new WebSocketServerProtocolHandler("/"));
                ctx.pipeline().addLast("webSocketHandler", new WebSocketHandler());
            } else {
                // add socket protocol handler
                ByteBuf buf = in.copy();
                buf.resetReaderIndex();
                out.add(buf);
                ctx.pipeline().addLast("lineSplit", new LineBasedFrameDecoder(Integer.MAX_VALUE));
                ctx.pipeline().addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
                ctx.pipeline().addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));
                ctx.pipeline().addLast("socketHandler", new SocketHandler());
            }

            in.resetReaderIndex();
            ctx.pipeline().remove(this.getClass());
        }

        private String getProtocol(ByteBuf in) {
            int length = in.readableBytes();
            if (length > MAX_LENGTH) {
                length = MAX_LENGTH;
            }
            in.markReaderIndex();
            byte[] content = new byte[length];
            in.readBytes(content);
            return new String(content);
        }
    }

    private class WebSocketHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            String message = ((TextWebSocketFrame) msg).text();
            if (message.startsWith(NettyWebServer.CMD_WEB_DISPLAY_DATA)) {
                LOGGER.info("start display data to web");
                ctx.channel().eventLoop().scheduleWithFixedDelay(new WebDisplayDataTask(ctx), 0, 1,
                    TimeUnit.SECONDS);
            } else {
                LOGGER.info("receive data from web: {}", message);
                inputDataQueue.put(message);
            }
        }
    }

    private class SocketHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            String message = String.valueOf(msg);
            if (message.startsWith(NettyWebServer.CMD_GET_DATA)) {
                LOGGER.info("start get data from web");
                ctx.channel().eventLoop().submit(new GetDataTask(ctx));
            } else {
                LOGGER.info("receive result from engine: {}", message);
                outputDataQueue.put(message);
            }
        }
    }

    private class GetDataTask implements Runnable {

        private final ChannelHandlerContext ctx;

        public GetDataTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String data = inputDataQueue.take();
                    ctx.channel().writeAndFlush(data + "\n");
                } catch (Exception e) {
                    LOGGER.error(null, e);
                }
            }
        }
    }

    private class WebDisplayDataTask implements Runnable {

        private final ChannelHandlerContext ctx;

        public WebDisplayDataTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            while (!outputDataQueue.isEmpty()) {
                try {
                    String data = outputDataQueue.take();
                    ctx.channel().writeAndFlush(new TextWebSocketFrame(data));
                } catch (Exception e) {
                    LOGGER.error(null, e);
                }
            }
        }
    }
}
