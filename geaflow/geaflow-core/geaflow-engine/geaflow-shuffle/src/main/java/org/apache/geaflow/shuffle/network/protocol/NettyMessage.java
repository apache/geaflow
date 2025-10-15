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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.Serializable;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.netty.NettyMessage.
 */
public abstract class NettyMessage implements Serializable {

    // ------------------------------------------------------------------------
    // Note: Every NettyMessage subtype needs to have a public 0-argument
    // constructor in order to work with the generic deserializer.
    // ------------------------------------------------------------------------

    // frame length (4), magic number (4), msg ID (1)
    public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;
    public static final int MAGIC_NUMBER = 0xBADC0FFE;

    protected static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
        return allocateBuffer(allocator, id, -1);
    }

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator     byte buffer allocator to use.
     * @param id            {@link NettyMessage} subclass ID.
     * @param contentLength content length (or <tt>-1</tt> if unknown).
     * @return a newly allocated direct buffer with header data written for decoder.
     */
    static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
        return allocateBuffer(allocator, id, 0, contentLength, true);
    }

    /**
     * Allocates a new buffer and adds some header information for the frame decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator           byte buffer allocator to use.
     * @param id                  {@link NettyMessage} subclass ID.
     * @param messageHeaderLength additional header length that should be part of the allocated
     *                            buffer and is written outside of this method.
     * @param contentLength       content length (or <tt>-1</tt> if unknown).
     * @param allocateForContent  whether to make room for the actual content in the buffer
     *                            (<tt>true</tt>) or whether to only return a buffer with the header information
     *                            (<tt>false</tt>).
     * @return a newly allocated direct buffer with header data written for decoder.
     */
    public static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id,
                                         int messageHeaderLength, int contentLength,
                                         boolean allocateForContent) {
        Preconditions.checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

        final ByteBuf buffer;
        if (!allocateForContent) {
            buffer = allocator.buffer(FRAME_HEADER_LENGTH + messageHeaderLength);
        } else if (contentLength != -1) {
            buffer = allocator.buffer(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
        } else {
            // Content length unknown -> start with the default initial size (rather than
            // FRAME_HEADER_LENGTH only):
            buffer = allocator.buffer();
        }
        // May be updated later, e.g. if contentLength == -1
        buffer.writeInt(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeByte(id);

        return buffer;
    }

    public abstract Object write(ByteBufAllocator allocator) throws Exception;


}
