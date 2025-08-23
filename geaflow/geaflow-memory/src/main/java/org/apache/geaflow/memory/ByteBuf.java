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

package org.apache.geaflow.memory;

import java.nio.ByteBuffer;

public class ByteBuf<T> {

    protected Chunk<T> chunk;
    protected long handle;
    protected int startOffset;
    protected int length;

    private ByteBuffer bf;

    public ByteBuf() {
    }

    public ByteBuf(Chunk<T> chunk, long handle, int startOffset, int length) {
        init(chunk, handle, startOffset, length);
    }

    void init(Chunk<T> chunk, long handle, int startOffset, int length) {
        this.chunk = chunk;
        this.handle = handle;
        this.startOffset = startOffset;
        this.length = length;
        this.bf = internalBuffer();
    }

    public ByteBuffer getBf() {
        return bf;
    }

    protected ByteBuffer internalBuffer() {

        ByteBuffer tmp = this.bf;
        if (tmp == null) {
            tmp = newInternalBuffer();
        }
        return tmp;
    }

    protected ByteBuffer newInternalBuffer() {
        ByteBuffer buffer;
        if (chunk.pool.getMemoryMode() == MemoryMode.OFF_HEAP) {
            buffer = (ByteBuffer) ((ByteBuffer) chunk.memory).duplicate().position(startOffset)
                .limit(startOffset + length);
        } else {
            buffer = ByteBuffer.wrap((byte[]) chunk.memory, startOffset, length);
        }
        return buffer.slice();
    }

    public void free(MemoryGroup group) {
        chunk.pool.free(chunk, handle, length);
        group.free(length, chunk.pool.getMemoryMode());
    }

    public Chunk<T> getChunk() {
        return chunk;
    }

    byte get(int pos) {
        return bf.get(pos);
    }

    int contentSize() {
        return bf.position();
    }

    public int getRemain() {
        return bf.remaining();
    }

    public long getHandle() {
        return handle;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getLength() {
        return length;
    }

    public boolean allocateFailed() {
        return chunk == null;
    }

    public void reset() {
        bf.clear();
    }

    public void position(int pos) {
        bf.position(pos);
    }

    public ByteBuffer duplicate() {
        return (ByteBuffer) getBf().duplicate().position(0).limit(bf.position());
    }

    public boolean hasRemaining() {
        return bf.hasRemaining();
    }
}
