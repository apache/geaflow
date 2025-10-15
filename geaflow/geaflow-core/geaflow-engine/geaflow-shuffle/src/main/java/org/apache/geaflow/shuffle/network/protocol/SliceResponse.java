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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.FileRegion;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;

public class SliceResponse extends NettyMessage {

    final PipeBuffer buffer;
    final ChannelId receiverId;
    final int sequenceNumber;
    final int bufferSize;

    public SliceResponse(PipeBuffer buffer, int sequenceNumber,
                         ChannelId inputChannelId) {
        this.buffer = buffer;
        this.sequenceNumber = sequenceNumber;
        this.receiverId = inputChannelId;
        this.bufferSize = buffer.getBuffer() != null ? buffer.getBufferSize() : 0;
    }

    public PipeBuffer getBuffer() {
        return buffer;
    }

    public ChannelId getReceiverId() {
        return receiverId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public Object write(ByteBufAllocator allocator) throws Exception {
        if (buffer.isData()) {
            int headerLen = 16 + 8 + 4 + 1;
            int contentSize = buffer.getBufferSize();
            // Only allocate header buffer - we will combine it with the data buffer below.
            ByteBuf headerBuf = allocateBuffer(allocator, MessageType.FETCH_SLICE_RESPONSE.getId(),
                headerLen, contentSize, false);

            receiverId.writeTo(headerBuf);
            headerBuf.writeLong(buffer.getBatchId());
            headerBuf.writeInt(sequenceNumber);
            headerBuf.writeBoolean(buffer.isData());

            int totalSize = headerBuf.readableBytes() + contentSize;
            headerBuf.setInt(0, totalSize);

            FileRegion body = buffer.getBuffer().toFileRegion();
            return new CompositeFileRegion(headerBuf, body, totalSize);
        } else {
            final ByteBuf result = allocateBuffer(allocator, MessageType.FETCH_SLICE_RESPONSE.getId());
            receiverId.writeTo(result);
            result.writeLong(buffer.getBatchId());
            result.writeInt(sequenceNumber);
            result.writeBoolean(buffer.isData());
            result.writeInt(buffer.getCount());
            result.writeBoolean(buffer.isFinish());
            result.setInt(0, result.readableBytes());
            return result;
        }

    }

    public static SliceResponse readFrom(ByteBuf buf) throws Exception {
        ChannelId inputChannelId = ChannelId.readFrom(buf);
        long batchId = buf.readLong();
        int sequenceNum = buf.readInt();
        boolean isData = buf.readBoolean();

        PipeBuffer recordBuffer;
        if (isData) {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            recordBuffer = new PipeBuffer(bytes, batchId);
        } else {
            int count = buf.readInt();
            boolean isFinish = buf.readBoolean();
            recordBuffer = new PipeBuffer(batchId, count, isFinish);
        }

        return new SliceResponse(recordBuffer, sequenceNum, inputChannelId);
    }

}
