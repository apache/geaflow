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

package org.apache.geaflow.shuffle.pipeline.slice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.shuffle.StorageLevel;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.ShuffleMemoryTracker;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.apache.geaflow.shuffle.storage.ShuffleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpillablePipelineSlice extends AbstractSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpillablePipelineSlice.class);

    private final String fileName;
    private final StorageLevel storageLevel;
    private final ShuffleStore store;
    private final ShuffleMemoryTracker memoryTracker;

    private OutputStream outputStream;
    private CloseableIterator<PipeBuffer> streamBufferIterator;
    private PipeBuffer value;
    private volatile boolean ready2read = false;

    // Bytes count in memory.
    private long memoryBytes = 0;
    // Bytes count on disk.
    private long diskBytes = 0;

    public SpillablePipelineSlice(String taskLogTag, SliceId sliceId) {
        this(taskLogTag, sliceId, ShuffleManager.getInstance().getShuffleConfig(),
            ShuffleManager.getInstance()
                .getShuffleMemoryTracker());
    }

    public SpillablePipelineSlice(String taskLogTag, SliceId sliceId,
                                  ShuffleConfig shuffleConfig, ShuffleMemoryTracker memoryTracker) {
        super(taskLogTag, sliceId);
        this.storageLevel = shuffleConfig.getStorageLevel();
        this.store = ShuffleStore.getShuffleStore(shuffleConfig);
        String fileName = String.format("shuffle-%d-%d-%d",
            sliceId.getPipelineId(), sliceId.getEdgeId(), sliceId.getSliceIndex());
        this.fileName = store.getFilePath(fileName);
        this.memoryTracker = memoryTracker;
    }

    public String getFileName() {
        return this.fileName;
    }

    //////////////////////////////
    // Produce data.

    /// ///////////////////////////

    @Override
    public boolean add(PipeBuffer buffer) {
        if (this.isReleased || this.ready2read) {
            throw new GeaflowRuntimeException("slice already released or mark finish: " + this.getSliceId());
        }
        totalBufferCount++;

        if (this.storageLevel == StorageLevel.MEMORY) {
            this.writeMemory(buffer);
            return true;
        }

        if (this.storageLevel == StorageLevel.DISK) {
            this.writeStore(buffer);
            return true;
        }

        this.writeMemory(buffer);
        if (!memoryTracker.checkMemoryEnough()) {
            this.spillWrite();
        }
        return true;
    }

    private void writeMemory(PipeBuffer buffer) {
        this.buffers.add(buffer);
        this.memoryBytes += buffer.getBufferSize();
    }

    private void writeStore(PipeBuffer buffer) {
        try {
            if (this.outputStream == null) {
                this.outputStream = store.getOutputStream(fileName);
            }
            this.write2Stream(buffer);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private void spillWrite() {
        try {
            if (this.outputStream == null) {
                this.outputStream = store.getOutputStream(fileName);
            }
            while (!buffers.isEmpty()) {
                PipeBuffer buffer = buffers.poll();
                write2Stream(buffer);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private void write2Stream(PipeBuffer buffer) throws IOException {
        OutBuffer outBuffer = buffer.getBuffer();
        if (outBuffer == null) {
            Encoders.INTEGER.encode(0, this.outputStream);
            Encoders.LONG.encode(buffer.getBatchId(), this.outputStream);
            Encoders.INTEGER.encode(buffer.getCount(), this.outputStream);
        } else {
            this.diskBytes += buffer.getBufferSize();
            Encoders.INTEGER.encode(outBuffer.getBufferSize(), this.outputStream);
            Encoders.LONG.encode(buffer.getBatchId(), this.outputStream);
            outBuffer.write(this.outputStream);
            outBuffer.release();
        }
    }

    @Override
    public void flush() {
        if (this.outputStream != null) {
            try {
                spillWrite();
                this.outputStream.flush();
                this.outputStream.close();
                this.outputStream = null;
            } catch (IOException e) {
                throw new GeaflowRuntimeException(e);
            }
            this.streamBufferIterator = new FileStreamIterator();
        }
        this.ready2read = true;
        LOGGER.info("write file {} {} {}", this.fileName, this.memoryBytes, this.diskBytes);
    }

    //////////////////////////////
    // Consume data.

    /// ///////////////////////////

    @Override
    public boolean hasNext() {
        if (this.isReleased) {
            return false;
        }
        if (this.value != null) {
            return true;
        }

        if (!buffers.isEmpty()) {
            this.value = buffers.poll();
            return true;
        }

        if (streamBufferIterator != null && streamBufferIterator.hasNext()) {
            this.value = streamBufferIterator.next();
            return true;
        }

        return false;
    }

    @Override
    public PipeBuffer next() {
        PipeBuffer next = this.value;
        this.value = null;
        return next;
    }

    @Override
    public boolean isReady2read() {
        return this.ready2read;
    }

    @Override
    public synchronized void release() {
        if (this.isReleased) {
            return;
        }
        this.buffers.clear();
        try {
            if (streamBufferIterator != null) {
                streamBufferIterator.close();
                streamBufferIterator = null;
            }
            Path path = Paths.get(this.fileName);
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        this.isReleased = true;
    }


    class FileStreamIterator implements CloseableIterator<PipeBuffer> {
        private InputStream inputStream;
        private PipeBuffer next;

        FileStreamIterator() {
            this.inputStream = store.getInputStream(fileName);
        }

        @Override
        public boolean hasNext() {
            if (this.next != null) {
                return true;
            }
            InputStream input = this.inputStream;
            try {
                if (input != null && input.available() > 0) {
                    int size = Encoders.INTEGER.decode(input);
                    long batchId = Encoders.LONG.decode(input);
                    if (size == 0) {
                        int count = Encoders.INTEGER.decode(input);
                        this.next = new PipeBuffer(batchId, count, true);
                        return true;
                    } else {
                        byte[] bytes = new byte[size];
                        int read = input.read(bytes);
                        if (read != bytes.length) {
                            String msg = String.format("illegal read size, expect %d, actual %d",
                                bytes.length, read);
                            throw new GeaflowRuntimeException(msg);
                        }
                        this.next = new PipeBuffer(bytes, batchId);
                        return true;
                    }
                }
            } catch (IOException e) {
                throw new GeaflowRuntimeException(e.getMessage(), e);
            }

            return false;
        }

        @Override
        public PipeBuffer next() {
            PipeBuffer buffer = this.next;
            this.next = null;
            return buffer;
        }

        @Override
        public void close() {
            if (inputStream != null) {
                try {
                    inputStream.close();
                    inputStream = null;
                } catch (IOException e) {
                    throw new GeaflowRuntimeException(e);
                }
            }
        }
    }

}
