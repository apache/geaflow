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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.geaflow.shuffle.network.protocol.AddCreditRequest;
import org.apache.geaflow.shuffle.network.protocol.BatchRequest;
import org.apache.geaflow.shuffle.network.protocol.CancelRequest;
import org.apache.geaflow.shuffle.network.protocol.CloseRequest;
import org.apache.geaflow.shuffle.network.protocol.ErrorResponse;
import org.apache.geaflow.shuffle.network.protocol.NettyMessage;
import org.apache.geaflow.shuffle.network.protocol.SliceRequest;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;
import org.apache.geaflow.shuffle.pipeline.slice.SequenceSliceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceRequestServerHandler.class);

    private final SliceOutputChannelHandler outboundQueue;

    public SliceRequestServerHandler(SliceOutputChannelHandler outboundQueue) {
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            if (msgClazz == SliceRequest.class) {
                SliceRequest request = (SliceRequest) msg;
                try {
                    SequenceSliceReader reader = new SequenceSliceReader(
                        request.getReceiverId(), outboundQueue);
                    reader.createSliceReader(request.getSliceId(), request.getStartBatchId(),
                        request.getInitialCredit());

                    outboundQueue.notifyReaderCreated(reader);
                } catch (Throwable notFound) {
                    respondWithError(ctx, notFound, request.getReceiverId());
                }
            } else if (msgClazz == CancelRequest.class) {
                CancelRequest request = (CancelRequest) msg;

                outboundQueue.cancel(request.receiverId());
            } else if (msgClazz == CloseRequest.class) {

                outboundQueue.close();
            } else if (msgClazz == BatchRequest.class) {
                BatchRequest request = (BatchRequest) msg;

                outboundQueue.applyReaderOperation(request.receiverId(),
                    reader -> reader.requestBatch(request.getNextBatchId()));
            } else if (msgClazz == AddCreditRequest.class) {
                AddCreditRequest request = (AddCreditRequest) msg;

                outboundQueue.applyReaderOperation(request.receiverId(),
                    reader -> reader.addCredit(request.getCredit()));
            } else {
                LOGGER.warn("Received unexpected client request: {}", msg);
                respondWithError(ctx, new IllegalArgumentException("unknown request:" + msg));
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new ErrorResponse(error));
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error,
                                  ChannelId sourceId) {
        ctx.writeAndFlush(new ErrorResponse(sourceId, error));
    }

}
