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

package org.apache.geaflow.shuffle.pipeline.buffer;

import io.netty.channel.FileRegion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.network.protocol.MemoryBytesFileRegion;

public class HeapBuffer extends AbstractBuffer {

    private static final int INITIAL_BUFFER_SIZE = 4096;
    private byte[] bytes;

    public HeapBuffer(byte[] bytes) {
        this(bytes, true);
    }

    public HeapBuffer(byte[] bytes, ShuffleMemoryTracker memoryTracker) {
        super(memoryTracker);
        this.bytes = bytes;
        this.requireMemory(bytes.length);
    }

    public HeapBuffer(byte[] bytes, boolean memoryTrack) {
        super(memoryTrack);
        this.bytes = bytes;
        this.requireMemory(bytes.length);
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public int getBufferSize() {
        return this.bytes == null ? 0 : this.bytes.length;
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
        if (this.bytes != null) {
            outputStream.write(this.bytes);
        }
    }

    @Override
    public FileRegion toFileRegion() {
        return new MemoryBytesFileRegion(this.bytes);
    }

    @Override
    public void release() {
        if (this.bytes != null) {
            int dataSize = this.bytes.length;
            releaseMemory(dataSize);
            this.bytes = null;
        }
    }

    public static class HeapBufferBuilder extends AbstractBufferBuilder {
        private ByteArrayOutputStream outputStream;

        @Override
        public OutputStream getOutputStream() {
            if (outputStream == null) {
                outputStream = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
            }
            return outputStream;
        }

        @Override
        public void positionStream(int position) {
        }

        @Override
        public int getBufferSize() {
            return this.outputStream != null ? outputStream.size() : 0;
        }

        @Override
        public OutBuffer build() {
            byte[] bytes = outputStream.toByteArray();
            this.outputStream.reset();
            this.resetRecordCount();
            return new HeapBuffer(bytes, this.memoryTrack);
        }

        @Override
        public void close() {
            if (this.outputStream != null) {
                try {
                    this.outputStream.close();
                } catch (IOException e) {
                    throw new GeaflowRuntimeException(e);
                }
                this.outputStream = null;
            }
        }
    }

}
