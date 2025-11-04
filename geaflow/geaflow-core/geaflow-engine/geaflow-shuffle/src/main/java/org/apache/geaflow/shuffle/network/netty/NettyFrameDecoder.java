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
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.LinkedList;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Spark's org.apache.spark.network.util.TransportFrameDecoder.
 */
public class NettyFrameDecoder extends ChannelInboundHandlerAdapter {

    private static final int LENGTH_SIZE = 4;
    private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
    private static final int UNKNOWN_FRAME_SIZE = -1;
    private static final long CONSOLIDATE_THRESHOLD = 20 * 1024 * 1024;

    private final LinkedList<ByteBuf> buffers = new LinkedList<>();
    private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);
    private final long consolidateThreshold;

    private CompositeByteBuf frameBuf = null;
    private long consolidatedFrameBufSize = 0;
    private int consolidatedNumComponents = 0;

    private long totalSize = 0;
    private long nextFrameSize = UNKNOWN_FRAME_SIZE;
    private int frameRemainingBytes = UNKNOWN_FRAME_SIZE;

    public NettyFrameDecoder() {
        this(CONSOLIDATE_THRESHOLD);
    }

    @VisibleForTesting
    NettyFrameDecoder(long consolidateThreshold) {
        this.consolidateThreshold = consolidateThreshold;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
        ByteBuf in = (ByteBuf) data;
        buffers.add(in);
        totalSize += in.readableBytes();

        while (!buffers.isEmpty()) {
            ByteBuf frame = decodeNext();
            if (frame == null) {
                break;
            }
            ctx.fireChannelRead(frame);
        }
    }

    private long decodeFrameSize() {
        if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
            return nextFrameSize;
        }

        // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
        // hold the bytes for the frame length in a composite buffer until we have enough data to read
        // the frame size. Normally, it should be rare to need more than one buffer to read the frame
        // size.
        ByteBuf first = buffers.getFirst();
        if (first.readableBytes() >= LENGTH_SIZE) {
            nextFrameSize = first.readInt() - LENGTH_SIZE;
            totalSize -= LENGTH_SIZE;
            if (!first.isReadable()) {
                buffers.removeFirst().release();
            }
            return nextFrameSize;
        }

        while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
            ByteBuf next = buffers.getFirst();
            int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
            frameLenBuf.writeBytes(next, toRead);
            if (!next.isReadable()) {
                buffers.removeFirst().release();
            }
        }

        nextFrameSize = frameLenBuf.readInt() - LENGTH_SIZE;
        totalSize -= LENGTH_SIZE;
        frameLenBuf.clear();
        return nextFrameSize;
    }

    private ByteBuf decodeNext() {
        long frameSize = decodeFrameSize();
        if (frameSize == UNKNOWN_FRAME_SIZE) {
            return null;
        }

        if (frameBuf == null) {
            Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE,
                "Too large frame: %s", frameSize);
            Preconditions.checkArgument(frameSize > 0,
                "Frame length should be positive: %s", frameSize);
            frameRemainingBytes = (int) frameSize;

            // If buffers is empty, then return immediately for more input data.
            if (buffers.isEmpty()) {
                return null;
            }
            // Otherwise, if the first buffer holds the entire frame, we attempt to
            // build frame with it and return.
            if (buffers.getFirst().readableBytes() >= frameRemainingBytes) {
                // Reset buf and size for next frame.
                frameBuf = null;
                nextFrameSize = UNKNOWN_FRAME_SIZE;
                return nextBufferForFrame(frameRemainingBytes);
            }
            // Other cases, create a composite buffer to manage all the buffers.
            frameBuf = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        }

        while (frameRemainingBytes > 0 && !buffers.isEmpty()) {
            ByteBuf next = nextBufferForFrame(frameRemainingBytes);
            frameRemainingBytes -= next.readableBytes();
            frameBuf.addComponent(true, next);
        }
        // If the delta size of frameBuf exceeds the threshold, then we do consolidation
        // to reduce memory consumption.
        if (frameBuf.capacity() - consolidatedFrameBufSize > consolidateThreshold) {
            int newNumComponents = frameBuf.numComponents() - consolidatedNumComponents;
            frameBuf.consolidate(consolidatedNumComponents, newNumComponents);
            consolidatedFrameBufSize = frameBuf.capacity();
            consolidatedNumComponents = frameBuf.numComponents();
        }
        if (frameRemainingBytes > 0) {
            return null;
        }

        return consumeCurrentFrameBuf();
    }

    private ByteBuf consumeCurrentFrameBuf() {
        final ByteBuf frame = frameBuf;
        // Reset buf and size for next frame.
        frameBuf = null;
        consolidatedFrameBufSize = 0;
        consolidatedNumComponents = 0;
        nextFrameSize = UNKNOWN_FRAME_SIZE;
        return frame;
    }

    /**
     * Takes the first buffer in the internal list, and either adjust it to fit in the frame
     * (by taking a slice out of it) or remove it from the internal list.
     */
    private ByteBuf nextBufferForFrame(int bytesToRead) {
        ByteBuf buf = buffers.getFirst();
        ByteBuf frame;

        if (buf.readableBytes() > bytesToRead) {
            frame = buf.retain().readSlice(bytesToRead);
            totalSize -= bytesToRead;
        } else {
            frame = buf;
            buffers.removeFirst();
            totalSize -= frame.readableBytes();
        }

        return frame;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        // Release all buffers that are still in our ownership.
        // Doing this in handlerRemoved(...) guarantees that this will happen in all cases:
        //     - When the Channel becomes inactive
        //     - When the decoder is removed from the ChannelPipeline
        for (ByteBuf b : buffers) {
            b.release();
        }
        buffers.clear();
        frameLenBuf.release();
        ByteBuf frame = consumeCurrentFrameBuf();
        if (frame != null) {
            frame.release();
        }
        super.handlerRemoved(ctx);
    }

}
