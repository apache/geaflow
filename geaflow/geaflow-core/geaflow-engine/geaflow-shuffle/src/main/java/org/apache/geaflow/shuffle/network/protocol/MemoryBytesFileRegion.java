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

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Spark's org.apache.spark.util.io.ChunkedByteBufferFileRegion.
 */
public class MemoryBytesFileRegion extends AbstractFileRegion {

    private ByteBuffer curBuffer;

    public MemoryBytesFileRegion(byte[] buffer) {
        super(buffer.length);
        this.curBuffer = ByteBuffer.wrap(buffer);
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        assert (position == transferred);
        if (position == contentSize) {
            return 0L;
        }
        boolean keepGoing = true;
        long written = 0L;
        ByteBuffer currentBuffer = curBuffer;

        while (keepGoing) {
            while (currentBuffer.hasRemaining() && keepGoing) {
                int ioSize = Math.min(currentBuffer.remaining(), chunkSize);
                int originalLimit = currentBuffer.limit();
                currentBuffer.limit(currentBuffer.position() + ioSize);
                int writtenSize = target.write(currentBuffer);
                currentBuffer.limit(originalLimit);
                written += writtenSize;
                if (writtenSize < ioSize) {
                    // the channel did not accept our entire write.  We do *not* keep trying -- netty wants
                    // us to just stop, and report how much we've written.
                    keepGoing = false;
                }
            }
            if (keepGoing) {
                curBuffer = null;
                break;
            }
        }

        transferred += written;
        return written;
    }

    @Override
    protected void deallocate() {

    }

}
