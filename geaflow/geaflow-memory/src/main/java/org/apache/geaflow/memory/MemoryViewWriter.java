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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryViewWriter {

    private static final Logger logger = LoggerFactory.getLogger(MemoryViewWriter.class);

    private final int spanSize;
    private final MemoryView view;

    public MemoryViewWriter(MemoryView view) {
        this(view, -1);
    }

    public MemoryViewWriter(MemoryView view, int spanSize) {
        this.view = view;
        this.spanSize = spanSize;
    }

    private void checkContentBounds(int len) {
        if (Integer.MAX_VALUE - view.contentSize < len) {
            throw new IllegalArgumentException(String.format(
                "current size %d plus write size %d should not be greater than Integer.MAX_VALUE %d",
                view.contentSize, len, Integer.MAX_VALUE));
        }
    }

    public void write(int b) {
        view.checkAvail();
        checkContentBounds(1);
        if (view.currIndex == view.bufList.size()) {
            span(1);
        }
        view.bufList.get(view.currIndex).getBf().put((byte) b);

        if (view.bufList.get(view.currIndex).contentSize() == view.bufList.get(view.currIndex)
            .getLength()) {
            view.currIndex++;
        }
        view.contentSize++;
    }

    public void write(byte[] bytes) {
        write(bytes, 0, bytes.length);
    }

    public void write(byte[] bytes, int off, int len) {
        view.checkAvail();
        if (len <= 0) {
            return;
        }
        checkContentBounds(len);
        int offset = off;
        int remain = len;
        while (remain > 0) {
            if (view.currIndex == view.bufList.size()) {
                span(remain);
            }
            int r = view.bufList.get(view.currIndex).getRemain();
            int toWrite = Math.min(r, remain);
            view.bufList.get(view.currIndex).getBf().put(bytes, offset, toWrite);
            view.contentSize += toWrite;

            remain -= toWrite;
            offset += toWrite;

            if (toWrite == r) {
                view.currIndex++;
            }
        }

    }

    public void write(ByteBuffer buffer, int len) throws IOException {
        view.checkAvail();
        checkContentBounds(len);

        ByteBuffer readBuf = buffer;
        int wrote = readBuf.position();
        int limit = readBuf.position() + len;
        Preconditions.checkArgument(limit <= readBuf.limit(),
            "length should not be bigger than buffer limit!");

        while (wrote < limit) {
            int remain = limit - wrote;
            if (view.currIndex == view.bufList.size()) {
                span(remain);
            }
            try {
                int r = view.bufList.get(view.currIndex).getRemain();
                int toWrite = Math.min(r, remain);
                ByteBuffer newBuf = (ByteBuffer) readBuf.duplicate().limit(wrote + toWrite);
                view.bufList.get(view.currIndex).getBf().put(newBuf);
                view.contentSize += toWrite;

                wrote += toWrite;
                readBuf.position(wrote);
                if (toWrite == r) {
                    view.currIndex++;
                }
            } catch (Throwable t) {
                logger.error(String.format("currIndex=%d,bufSize=%d,currBuf=%s", view.currIndex,
                    view.bufList.size(), view.bufList.get(view.currIndex).getBf() == null), t);
                throw t;
            }
        }
    }

    public void write(InputStream inputStream, int len) throws IOException {
        view.checkAvail();
        if (len <= 0) {
            return;
        }
        checkContentBounds(len);

        int remain = len;
        byte[] buffer = new byte[1024];
        while (remain > 0) {
            if (view.currIndex == view.bufList.size()) {
                span(remain);
            }
            try {
                int r = view.bufList.get(view.currIndex).getRemain();
                int toWrite = Math.min(Math.min(r, remain), 1024);

                inputStream.read(buffer, 0, toWrite);
                view.bufList.get(view.currIndex).getBf().put(buffer, 0, toWrite);
                view.contentSize += toWrite;
                remain -= toWrite;

                if (toWrite == r) {
                    view.currIndex++;
                }
            } catch (Throwable t) {
                logger.error(String.format("currIndex=%d,bufSize=%d,currBuf=%s", view.currIndex,
                    view.bufList.size(), view.bufList.get(view.currIndex).getBf() == null), t);
                throw t;
            }
        }
    }

    private void span(int size) {
        List<ByteBuf> bufs = MemoryManager.getInstance()
            .requireBufs(spanSize > 0 ? spanSize : size, view.group);
        view.bufList.addAll(bufs);
    }

    public void position(int position) {
        if (position > view.contentSize || position < 0) {
            throw new IllegalArgumentException(
                String.format("position %s not in [0,%s]", position, view.contentSize));
        }
        if (position == view.contentSize) {
            return;
        }
        if (position == 0) {
            view.reset();
            return;
        }
        int i = 0;
        int totalSize = 0;
        for (; i < view.bufList.size(); i++) {
            int length = view.bufList.get(i).getLength();
            if (length + totalSize > position) {
                break;
            }
            totalSize += length;
        }
        view.currIndex = i;
        view.contentSize = position;

        view.bufList.get(i).position(position - totalSize);
        for (int j = i + 1; j < view.bufList.size(); j++) {
            view.bufList.get(j).reset();
        }
    }

}
