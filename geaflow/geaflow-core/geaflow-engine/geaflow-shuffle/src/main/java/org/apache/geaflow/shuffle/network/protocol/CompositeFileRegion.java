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
import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * This class is an adaptation of Spark's org.apache.spark.network.protocol.MessageWithHeader.
 */
public class CompositeFileRegion extends AbstractFileRegion {

    private final ByteBuf header;
    private final int headerLength;
    private final Object body;
    private long totalBytesTransferred;

    // When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
    // The size should not be too large as it will waste underlying memory copy. e.g. If network
    // available buffer is smaller than this limit, the data cannot be sent within one single write
    // operation while it still will make memory copy with this size.
    private static final int NIO_BUFFER_LIMIT = 256 * 1024;

    /**
     * @param header      the message header.
     * @param body        the message body. Must be either a {@link ByteBuf} or a {@link FileRegion}.
     * @param contentSize the length of the message body and header, in bytes.
     */
    public CompositeFileRegion(ByteBuf header, Object body, long contentSize) {
        super(contentSize);
        Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
            "Body must be a ByteBuf or a FileRegion.");
        this.header = header;
        this.headerLength = header.readableBytes();
        this.body = body;
    }

    @Override
    public long transferred() {
        return totalBytesTransferred;
    }

    /**
     * This code is more complicated than you would think because we might require multiple
     * transferTo invocations in order to transfer a single CompositeMessage to avoid busy waiting.
     *
     * <p>The contract is that the caller will ensure position is properly set to the total number
     * of bytes transferred so far (i.e. value returned by transferred()).
     */
    @Override
    public long transferTo(final WritableByteChannel target, final long position) throws IOException {
        Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position.");
        // Bytes written for header in this call.
        long writtenHeader = 0;
        if (header.readableBytes() > 0) {
            writtenHeader = copyByteBuf(header, target);
            totalBytesTransferred += writtenHeader;
            if (header.readableBytes() > 0) {
                return writtenHeader;
            }
        }

        // Bytes written for body in this call.
        long writtenBody = 0;
        if (body instanceof FileRegion) {
            writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);
        } else if (body instanceof ByteBuf) {
            writtenBody = copyByteBuf((ByteBuf) body, target);
        }
        totalBytesTransferred += writtenBody;

        return writtenHeader + writtenBody;
    }

    private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
        int length = Math.min(buf.readableBytes(), NIO_BUFFER_LIMIT);
        // If the ByteBuf holds more then one ByteBuffer we should better call nioBuffers(...)
        // to eliminate extra memory copies.
        int written = 0;
        if (buf.nioBufferCount() == 1) {
            ByteBuffer buffer = buf.nioBuffer(buf.readerIndex(), length);
            written = target.write(buffer);
        } else {
            ByteBuffer[] buffers = buf.nioBuffers(buf.readerIndex(), length);
            for (ByteBuffer buffer : buffers) {
                int remaining = buffer.remaining();
                int w = target.write(buffer);
                written += w;
                if (w < remaining) {
                    // Could not write all, we need to break now.
                    break;
                }
            }
        }
        buf.skipBytes(written);
        return written;
    }

    @Override
    protected void deallocate() {
        header.release();
        ReferenceCountUtil.release(body);
    }

    @Override
    public CompositeFileRegion touch(Object o) {
        super.touch(o);
        header.touch(o);
        ReferenceCountUtil.touch(body, o);
        return this;
    }

    @Override
    public CompositeFileRegion retain(int increment) {
        super.retain(increment);
        header.retain(increment);
        ReferenceCountUtil.retain(body, increment);
        return this;
    }

    @Override
    public boolean release(int decrement) {
        header.release(decrement);
        ReferenceCountUtil.release(body, decrement);
        return super.release(decrement);
    }

}
