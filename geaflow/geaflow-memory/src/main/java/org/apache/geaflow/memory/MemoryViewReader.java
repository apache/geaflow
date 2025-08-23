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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;

public class MemoryViewReader {

    private int readBufPos = 0;
    private int readIndex = 0;
    private int readPos = 0;
    private final MemoryView view;

    public MemoryViewReader(MemoryView view) {
        this.view = view;
        view.checkAvail();
    }

    public boolean hasNext() {
        return view.contentSize() > readPos;
    }

    public byte read() {
        if (readPos >= view.contentSize()) {
            throw new BufferUnderflowException();
        }
        byte res = view.bufList.get(readIndex).get(readBufPos++);

        if (readBufPos == view.bufList.get(readIndex).getLength()) {
            readIndex++;
            readBufPos = 0;
        }
        readPos++;
        return res;
    }

    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    public int read(OutputStream outputStream) throws IOException {
        int readLen = 0;
        while (readIndex < view.bufList.size()) {
            ByteBuf buf = view.bufList.get(readIndex);
            int toRead = buf.contentSize() - readBufPos;
            readLen += toRead;

            if (buf.chunk.pool.getMemoryMode() == MemoryMode.ON_HEAP) {
                outputStream.write(buf.getBf().array(), buf.getStartOffset(), toRead);
            } else {
                byte[] b = new byte[toRead];
                buf.duplicate().get(b, 0, toRead);
                outputStream.write(b, 0, toRead);
            }

            readBufPos = 0;
            readIndex++;
        }
        readPos += readLen;
        return readLen;
    }

    public int read(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int offset = off;
        int remain = len;

        while (remain > 0 && readIndex < view.bufList.size()) {
            ByteBuf buf = view.bufList.get(readIndex);
            int r = buf.contentSize() - readBufPos;
            if (r == 0) {
                break;
            }
            int toRead = Math.min(r, remain);
            if (buf.getChunk().pool.getMemoryMode() == MemoryMode.ON_HEAP) {
                System.arraycopy(buf.getBf().array(), buf.getStartOffset() + readBufPos, b, offset,
                    toRead);
            } else {
                DirectMemory.copyMemory(DirectMemory.directBufferAddress(buf.getBf()) + readBufPos,
                    b, offset, toRead);
            }
            remain -= toRead;
            offset += toRead;
            readBufPos += toRead;

            if (toRead == r && !buf.hasRemaining()) {
                readBufPos = 0;
                readIndex++;
            }

        }
        int readLen = len - remain;
        readPos += readLen;
        return readLen == 0 ? -1 : readLen;
    }

    public long skip(long n) {
        int remain = (int) n;
        while (remain > 0 && readIndex < view.bufList.size()) {
            ByteBuf buf = view.bufList.get(readIndex);
            int toRead = Math.min(buf.contentSize() - readBufPos, remain);
            remain -= toRead;
            readBufPos += toRead;
            if (readBufPos == buf.contentSize()) {
                if (buf.hasRemaining()) {
                    break;
                }
                readBufPos = 0;
                readIndex++;
            }
        }
        long len = n - remain;
        readPos += len;
        return len;
    }

    public void reset() {
        this.readIndex = 0;
        this.readBufPos = 0;
        this.readPos = 0;
    }

    public int readPos() {
        return readPos;
    }

    public int available() {
        return view.contentSize - readPos;
    }
}
