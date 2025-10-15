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

package org.apache.geaflow.shuffle.api.writer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer.BufferBuilder;
import org.apache.geaflow.shuffle.pipeline.slice.BlockingSlice;
import org.apache.geaflow.shuffle.pipeline.slice.IPipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineShardWriter<T> extends ShardWriter<T, Shard> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineWriter.class);

    private OutputFlusher outputFlusher;
    private final AtomicReference<Throwable> throwable;
    private final AtomicInteger curBufferBytes;
    private int maxWriteBufferSize;

    public PipelineShardWriter() {
        this.throwable = new AtomicReference<>();
        this.curBufferBytes = new AtomicInteger(0);
    }

    @Override
    public void init(IWriterContext writerContext) {
        super.init(writerContext);
        String threadName = String.format("flusher-%s", writerContext.getTaskName());
        int flushTimeout = this.shuffleConfig.getFlushBufferTimeoutMs();
        this.maxWriteBufferSize = shuffleConfig.getMaxWriteBufferSize();
        this.outputFlusher = new OutputFlusher(threadName, flushTimeout);
        this.outputFlusher.start();
    }

    @Override
    protected IPipelineSlice newSlice(String taskLogTag, SliceId sliceId) {
        if (enableBackPressure) {
            return new BlockingSlice(taskLogTag, sliceId, this);
        } else {
            return new PipelineSlice(taskLogTag, sliceId);
        }
    }

    @Override
    public void emit(long batchId, T value, boolean isRetract, int[] channels) throws IOException {
        this.checkError();
        super.emit(batchId, value, isRetract, channels);
    }

    @Override
    public void emit(long batchId, List<T> data, int channel) throws IOException {
        this.checkError();
        super.emit(batchId, data, channel);
    }

    @Override
    public Optional<Shard> doFinish(long windowId) throws IOException {
        this.checkError();
        return Optional.empty();
    }

    @Override
    protected void sendBuffer(int sliceIndex, BufferBuilder builder, long windowId) {
        if (enableBackPressure) {
            if (curBufferBytes.get() >= maxWriteBufferSize) {
                synchronized (this) {
                    while (curBufferBytes.get() >= maxWriteBufferSize) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            throw new GeaflowRuntimeException(e);
                        }
                    }
                }
            }
            curBufferBytes.addAndGet(builder.getBufferSize());
        }
        super.sendBuffer(sliceIndex, builder, windowId);
    }

    public void notifyBufferConsumed(int bufferBytes) {
        int preBytes = curBufferBytes.getAndAdd(-bufferBytes);
        if (preBytes >= maxWriteBufferSize && curBufferBytes.get() < maxWriteBufferSize) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    private void flushAll() {
        boolean flushed = this.flushSlices();
        if (!flushed) {
            LOGGER.warn("terminate flusher due to slices released");
            this.outputFlusher.terminate();
        }
    }

    @Override
    public void close() {
        if (this.outputFlusher != null) {
            this.outputFlusher.terminate();
            this.outputFlusher = null;
        }
    }

    private void checkError() throws IOException {
        if (this.throwable.get() != null) {
            Throwable t = this.throwable.get();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new GeaflowRuntimeException(t);
            }
        }
    }

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
            LOGGER.info("start {} with timeout {}ms", name, timeout);
        }

        public void terminate() {
            if (running) {
                running = false;
                interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (this.running) {
                    try {
                        Thread.sleep(this.timeout);
                    } catch (InterruptedException e) {
                        // Propagate this if we are still running,
                        // because it should not happen in that case.
                        if (this.running) {
                            LOGGER.error("Interrupted", e);
                            throw e;
                        }
                    }

                    // Any errors here should let the thread come to a halt and be
                    // recognized by the writer.
                    flushAll();
                }
                flushAll();
            } catch (Throwable t) {
                if (throwable.compareAndSet(null, t)) {
                    LOGGER.error("flush failed", t);
                }
            }
        }
    }

}
