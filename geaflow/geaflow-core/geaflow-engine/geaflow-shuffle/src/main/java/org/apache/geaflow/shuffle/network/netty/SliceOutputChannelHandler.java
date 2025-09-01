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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.geaflow.shuffle.network.protocol.ErrorResponse;
import org.apache.geaflow.shuffle.network.protocol.SliceResponse;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;
import org.apache.geaflow.shuffle.pipeline.slice.SequenceSliceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceOutputChannelHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceOutputChannelHandler.class);

    // The readers which are already enqueued available for transferring data.
    private final ArrayDeque<SequenceSliceReader> availableReaders = new ArrayDeque<>();

    // All the readers created for the consumers' slice requests.
    private final ConcurrentMap<ChannelId, SequenceSliceReader> allReaders =
        new ConcurrentHashMap<>();

    private boolean fatalError;

    private ChannelHandlerContext ctx;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

    public void notifyNonEmpty(final SequenceSliceReader reader) {
        ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
    }

    /**
     * Try to enqueue the reader once receiving non-empty reader notification from the sliceWriter.
     *
     * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
     * availability, so there is no race condition here.
     */
    private void enqueueReader(final SequenceSliceReader reader) throws Exception {
        if (reader.isRegistered() || !reader.isAvailable()) {
            return;
        }

        // Queue an available reader for consumption. If the queue is empty,
        // we try trigger the actual write. Otherwise, this will be handled by
        // the writeAndFlushNextMessageIfPossible calls.
        boolean triggerWrite = availableReaders.isEmpty();
        addAvailableReader(reader);

        if (triggerWrite) {
            writeAndFlushNextMessageIfPossible(ctx.channel());
        }
    }

    public void notifyReaderCreated(final SequenceSliceReader reader) {
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(ChannelId receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        for (SequenceSliceReader reader : allReaders.values()) {
            releaseReader(reader);
        }
        allReaders.clear();
    }

    public void applyReaderOperation(ChannelId receiverId, Consumer<SequenceSliceReader> operation)
        throws Exception {
        if (fatalError) {
            return;
        }

        SequenceSliceReader reader = allReaders.get(receiverId);
        if (reader != null) {
            operation.accept(reader);
            enqueueReader(reader);
        } else {
            throw new IllegalStateException(
                "No reader for receiverId = " + receiverId + " exists.");
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.

        if (msg instanceof SequenceSliceReader) {
            enqueueReader((SequenceSliceReader) msg);
        } else if (msg.getClass() == ChannelId.class) {
            // Release reader that get a cancel request.
            ChannelId toCancel = (ChannelId) msg;

            // Remove reader from queue of available readers.
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // Remove reader from queue of all readers and release its resource.
            final SequenceSliceReader toRelease = allReaders.remove(toCancel);
            if (toRelease != null) {
                releaseReader(toRelease);
            }
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        if (fatalError || !channel.isWritable()) {
            return;
        }

        PipeChannelBuffer next;
        SequenceSliceReader reader = null;

        try {
            while (true) {
                reader = pollAvailableReader();

                // No queue with available data. We allow this here, because
                // of the write callbacks that are executed after each write.
                if (reader == null) {
                    return;
                }

                next = reader.next();
                if (next != null) {
                    // This channel was now removed from the available reader queue.
                    // We re-add it into the queue if it is still available.
                    if (next.moreAvailable()) {
                        addAvailableReader(reader);
                    }

                    SliceResponse msg = new SliceResponse(next.getBuffer(),
                        reader.getSequenceNumber(), reader.getReceiverId());

                    // Write and flush and wait until this is done before
                    // trying to continue with the next buffer.
                    channel.writeAndFlush(msg).addListener(new WriteNextMessageIfPossibleListener(next.getBuffer()));

                    return;
                }
            }
        } catch (Throwable t) {
            LOGGER.error("fetch {} failed: {}", reader, t.getMessage());
            throw new IOException(t.getMessage(), t);
        }
    }

    private void addAvailableReader(SequenceSliceReader reader) {
        availableReaders.add(reader);
        reader.setRegistered(true);
    }

    private SequenceSliceReader pollAvailableReader() {
        SequenceSliceReader reader = availableReaders.poll();
        if (reader != null) {
            reader.setRegistered(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.warn("channel inactive and release resource...");
        releaseAllResources();
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOGGER.error("Encountered error while consuming slices", cause);

        fatalError = true;
        releaseAllResources();

        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void releaseAllResources() throws IOException {
        // Note: this is only ever executed by one thread: the Netty IO thread!
        for (SequenceSliceReader reader : allReaders.values()) {
            releaseReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseReader(SequenceSliceReader reader) throws IOException {
        reader.setRegistered(false);
        reader.releaseAllResources();
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the queues.
    private class WriteNextMessageIfPossibleListener implements ChannelFutureListener {

        private final OutBuffer buffer;

        public WriteNextMessageIfPossibleListener(PipeBuffer pipeBuffer) {
            this.buffer = pipeBuffer.getBuffer();
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (buffer != null) {
                    buffer.release();
                }
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else if (future.cause() != null) {
                    handleException(future.channel(), future.cause());
                } else {
                    handleException(future.channel(),
                        new IllegalStateException("Sending cancelled by user."));
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }

}
