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

package org.apache.geaflow.shuffle.network.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import org.apache.geaflow.memory.ByteBuf;
import org.apache.geaflow.memory.MemoryView;

public class MemoryViewFileRegion extends AbstractFileRegion {

    private final Iterator<ByteBuf> bufIterator;
    private ByteBuffer curBuffer;

    public MemoryViewFileRegion(MemoryView memoryView) {
        super(memoryView.contentSize());
        this.bufIterator = memoryView.getBufList().iterator();
        this.curBuffer = this.getNextBuffer();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (position == this.contentSize) {
            return 0L;
        }
        boolean keepGoing = true;
        long written = 0L;
        ByteBuffer currentBuffer = this.curBuffer;
        while (keepGoing) {
            while (currentBuffer.hasRemaining() && keepGoing) {
                int ioSize = Math.min(currentBuffer.remaining(), this.chunkSize);
                int originalLimit = currentBuffer.limit();
                currentBuffer.limit(currentBuffer.position() + ioSize);
                int writtenSize = target.write(currentBuffer);
                currentBuffer.limit(originalLimit);
                written += writtenSize;
                if (writtenSize < ioSize) {
                    keepGoing = false;
                }
            }
            if (keepGoing) {
                if (!this.bufIterator.hasNext()) {
                    keepGoing = false;
                } else {
                    currentBuffer = this.getNextBuffer();
                }
            }
        }

        this.transferred += written;
        return written;
    }

    @Override
    protected void deallocate() {
    }

    private ByteBuffer getNextBuffer() {
        ByteBuffer buffer = this.bufIterator.next().getBf().duplicate();
        buffer.flip();
        this.curBuffer = buffer;
        return buffer;
    }

}
