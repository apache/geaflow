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

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.shuffle.network.protocol.CancelRequest;
import org.apache.geaflow.shuffle.network.protocol.ErrorResponse;
import org.apache.geaflow.shuffle.network.protocol.NettyMessage;
import org.apache.geaflow.shuffle.network.protocol.SliceResponse;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;
import org.apache.geaflow.shuffle.pipeline.channel.RemoteInputChannel;
import org.apache.geaflow.shuffle.util.SliceNotFoundException;
import org.apache.geaflow.shuffle.util.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.
 */
public class SliceRequestClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceRequestClientHandler.class);

    // Channels, which already requested partitions from the producers.
    private final ConcurrentMap<ChannelId, RemoteInputChannel> inputChannels =
        new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> channelError = new AtomicReference<>();

    // Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
    // while data is still coming in for this channel.
    private final ConcurrentMap<ChannelId, ChannelId> cancelled = new ConcurrentHashMap<>();

    // The channel handler context is initialized in channel active event by netty thread
    // the context may also be accessed by task thread or canceler thread to
    // cancel partition request during releasing resources.
    private volatile ChannelHandlerContext ctx;

    // ------------------------------------------------------------------------
    // Input channel/receiver registration
    // ------------------------------------------------------------------------

    public void addInputChannel(RemoteInputChannel listener) throws IOException {
        checkError();
        inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
    }

    public void removeInputChannel(RemoteInputChannel listener) {
        inputChannels.remove(listener.getInputChannelId());
    }

    public RemoteInputChannel getInputChannel(ChannelId inputChannelId) {
        return inputChannels.get(inputChannelId);
    }

    public void cancelRequest(ChannelId inputChannelId) {
        if (inputChannelId == null || ctx == null) {
            return;
        }

        if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
            ctx.writeAndFlush(new CancelRequest(inputChannelId));
        }
    }

    // ------------------------------------------------------------------------
    // Network events
    // ------------------------------------------------------------------------

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Unexpected close. In normal operation, the client closes the connection after all input
        // channels have been removed. This indicates a problem with the remote server.
        if (!inputChannels.isEmpty()) {
            final SocketAddress remoteAddr = ctx.channel().remoteAddress();

            notifyAllChannelsOfErrorAndClose(new TransportException(
                "Connection unexpectedly closed by remote server '" + remoteAddr + "'. "
                    + "This might indicate that the remote server was lost.", remoteAddr));
        }

        super.channelInactive(ctx);
    }

    /**
     * Called on exceptions in the client handler pipeline.
     *
     * <p>Remote exceptions are received as regular payload.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof TransportException) {
            notifyAllChannelsOfErrorAndClose(cause);
        } else {
            final SocketAddress remoteAddr = ctx.channel().remoteAddress();
            final TransportException tex;

            // Improve on the connection reset by peer error message.
            if (cause instanceof IOException && "Connection reset by peer"
                .equals(cause.getMessage())) {
                tex = new TransportException("Lost connection to server '" + remoteAddr + "'. "
                    + "This indicates that the remote server was lost.", remoteAddr, cause);
            } else {
                final SocketAddress localAddr = ctx.channel().localAddress();
                tex = new TransportException(
                    String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr),
                    localAddr, cause);
            }

            notifyAllChannelsOfErrorAndClose(tex);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                NettyMessage nettyMessage) throws Exception {
        try {
            decodeMsg(nettyMessage);
        } catch (Throwable t) {
            notifyAllChannelsOfErrorAndClose(t);
        }
    }

    private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
        if (channelError.compareAndSet(null, cause)) {
            try {
                for (RemoteInputChannel inputChannel : inputChannels.values()) {
                    inputChannel.onError(cause);
                }
            } catch (Throwable t) {
                LOGGER.warn(
                    "Exception was thrown during error notification of a remote input channel.", t);
            } finally {
                inputChannels.clear();

                if (ctx != null) {
                    ctx.close();
                }
            }
        }
    }

    /**
     * Checks for an error and rethrows it if one was reported.
     */
    @VisibleForTesting
    void checkError() throws IOException {
        final Throwable t = channelError.get();

        if (t != null) {
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("There has been an error in the channel.", t);
            }
        }
    }

    private void decodeMsg(Object msg) {
        final Class<?> msgClazz = msg.getClass();

        if (msgClazz == SliceResponse.class) {
            SliceResponse response = (SliceResponse) msg;

            RemoteInputChannel inputChannel = inputChannels.get(response.getReceiverId());
            if (inputChannel == null || inputChannel.isReleased()) {
                cancelRequest(response.getReceiverId());
                return;
            }

            try {
                processBuffer(inputChannel, response);
            } catch (Throwable t) {
                inputChannel.onError(t);
            }
        } else if (msgClazz == ErrorResponse.class) {
            ErrorResponse error = (ErrorResponse) msg;
            SocketAddress remoteAddr = ctx.channel().remoteAddress();

            if (error.isFatalError()) {
                notifyAllChannelsOfErrorAndClose(
                    new TransportException("Fatal error at remote server '" + remoteAddr + "'.",
                        remoteAddr, error.getCause()));
            } else {
                RemoteInputChannel inputChannel = inputChannels.get(error.getChannelId());

                if (inputChannel != null) {
                    if (error.getCause().getClass() == SliceNotFoundException.class) {
                        inputChannel.onFailedFetchRequest();
                    } else {
                        inputChannel.onError(
                            new TransportException("Error at remote server '" + remoteAddr + "'.",
                                remoteAddr, error.getCause()));
                    }
                }
            }
        } else {
            throw new IllegalStateException(
                "Received unknown message from producer: " + msg.getClass());
        }
    }

    private void processBuffer(RemoteInputChannel inputChannel, SliceResponse response)
        throws Throwable {
        if (response.getBuffer().isData() && response.getBufferSize() == 0) {
            inputChannel.onEmptyBuffer(response.getSequenceNumber());
        } else if (response.getBuffer() != null) {
            inputChannel.onBuffer(response.getBuffer(), response.getSequenceNumber());
        } else {
            throw new IllegalStateException(
                "The read buffer is null in input channel: " + inputChannel.getChannelIndex());
        }
    }

}
