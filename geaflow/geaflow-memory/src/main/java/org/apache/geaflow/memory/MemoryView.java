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
import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.memory.channel.ByteBufferWritableChannel;

public class MemoryView implements Serializable, Closeable {

    final List<ByteBuf> bufList;
    int contentSize;
    int currIndex = 0; // -1 indicates released

    final MemoryGroup group;

    private transient MemoryViewWriter writer; // only one writer

    public MemoryView(List<ByteBuf> bufList, MemoryGroup memoryGroup) {
        Preconditions.checkArgument(bufList != null && bufList.size() > 0);
        this.bufList = new ArrayList<>(bufList);
        this.group = memoryGroup;
    }

    public List<ByteBuf> getBufList() {
        return bufList;
    }

    public byte[] toArray() {
        checkAvail();
        ByteBufferWritableChannel channel = new ByteBufferWritableChannel(contentSize);
        writeFully(channel);
        channel.close();
        return channel.getData();
    }

    private void writeFully(ByteBufferWritableChannel channel) {
        // https://www.evanjones.ca/java-bytebuffer-leak.html
        // carefully set write number
        int maxIndex = Math.min(bufList.size() - 1, currIndex);
        for (int i = 0; i <= maxIndex; i++) {
            synchronized (bufList.get(i)) {
                channel.write(bufList.get(i).getBf(), 0);
            }
        }
    }

    void checkAvail() {
        Preconditions.checkArgument(currIndex != -1, "view has been released");
    }

    public int remain() {
        checkAvail();
        int remain = 0;
        for (int i = currIndex; i < bufList.size(); i++) {
            remain += bufList.get(i).getBf().remaining();
        }
        return remain;
    }

    public int contentSize() {
        return contentSize;
    }

    @Override
    public int hashCode() {
        int result = 1;
        int maxIndex = Math.min(bufList.size() - 1, currIndex);
        for (int i = 0; i <= maxIndex; i++) {
            ByteBuf buf = bufList.get(i);
            for (int j = 0; j < buf.getBf().capacity(); j++) {
                result = 31 * result + buf.getBf().get(j);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MemoryView)) {
            return false;
        }
        if (this == other) {
            return true;
        }

        MemoryView otherView = (MemoryView) other;

        if (group != otherView.group) {
            return false;
        }

        if (contentSize != otherView.contentSize) {
            return false;
        }

        MemoryViewReader reader = new MemoryViewReader(this);
        MemoryViewReader reader2 = new MemoryViewReader(otherView);

        for (int i = 0; i < contentSize; i++) {
            if (reader.read() != reader2.read()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void close() {
        if (currIndex >= 0) {
            currIndex = -1; // must be after untegister
            bufList.forEach(buf -> buf.free(group));
            group.updateByteBufCount(-1 * bufList.size());
            bufList.clear();
        }
    }

    public MemoryGroup getGroup() {
        return this.group;
    }

    public MemoryViewReader getReader() {
        return new MemoryViewReader(this);
    }

    public MemoryViewWriter getWriter() {
        if (writer == null) {
            writer = new MemoryViewWriter(this, group.getSpanSize());
        }
        return writer;
    }

    public MemoryViewWriter getWriter(int spanSize) {
        if (writer == null) {
            writer = new MemoryViewWriter(this, spanSize);
        }
        return writer;
    }

    public void reset() {
        currIndex = 0;
        contentSize = 0;
        bufList.forEach(ByteBuf::reset);
    }
}
