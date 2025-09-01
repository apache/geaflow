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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.memory.channel.ByteArrayInputStream;
import org.apache.geaflow.memory.channel.ByteArrayOutputStream;
import org.apache.geaflow.shuffle.network.protocol.MemoryViewFileRegion;

public class MemoryViewBuffer extends AbstractBuffer {

    private final MemoryView memoryView;

    public MemoryViewBuffer(MemoryView memoryView, boolean memoryTrack) {
        super(memoryTrack);
        this.memoryView = memoryView;
        this.requireMemory(memoryView.contentSize());
    }

    public MemoryViewBuffer(MemoryView memoryView, ShuffleMemoryTracker memoryTracker) {
        super(memoryTracker);
        this.memoryView = memoryView;
        this.requireMemory(memoryView.contentSize());
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.memoryView);
    }

    @Override
    public FileRegion toFileRegion() {
        return new MemoryViewFileRegion(this.memoryView);
    }

    @Override
    public int getBufferSize() {
        return this.memoryView.contentSize();
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
        if (this.memoryView.contentSize() > 0) {
            this.memoryView.getReader().read(outputStream);
        }
    }

    @Override
    public void release() {
        int contentSize = memoryView.contentSize();
        this.memoryView.close();
        this.releaseMemory(contentSize);
    }

    public static class MemoryViewBufferBuilder extends AbstractBufferBuilder {

        private MemoryView memoryView;
        private OutputStream outputStream;

        @Override
        public OutputStream getOutputStream() {
            if (this.memoryView == null) {
                this.memoryView = MemoryManager.getInstance().requireMemory(
                    MemoryGroupManger.SHUFFLE.getSpanSize(), MemoryGroupManger.SHUFFLE);
                this.outputStream = new ByteArrayOutputStream(this.memoryView);
            }
            return this.outputStream;
        }

        @Override
        public void positionStream(int position) {
        }

        @Override
        public int getBufferSize() {
            return this.memoryView == null ? 0 : this.memoryView.contentSize();
        }

        @Override
        public OutBuffer build() {
            final MemoryViewBuffer buffer = new MemoryViewBuffer(this.memoryView, this.memoryTrack);
            this.memoryView = null;
            this.outputStream = null;
            this.resetRecordCount();
            return buffer;
        }

        @Override
        public void close() {
        }

    }

}
