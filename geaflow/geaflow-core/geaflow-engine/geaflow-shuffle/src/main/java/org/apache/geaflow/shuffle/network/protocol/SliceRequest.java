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
import java.io.IOException;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;

public class SliceRequest extends NettyMessage {

    final SliceId sliceId;
    final long startBatchId;
    final ChannelId receiverId;
    final int initialCredit;

    public SliceRequest(SliceId sliceId, long startBatchId, ChannelId receiverId,
                        int initialCredits) {
        this.sliceId = sliceId;
        this.startBatchId = startBatchId;
        this.receiverId = receiverId;
        this.initialCredit = initialCredits;
    }

    public ChannelId getReceiverId() {
        return receiverId;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public long getStartBatchId() {
        return startBatchId;
    }

    public int getInitialCredit() {
        return initialCredit;
    }

    @Override
    public ByteBuf write(ByteBufAllocator allocator) throws IOException {
        ByteBuf result = null;

        try {
            int length =
                SliceId.SLICE_ID_BYTES + ChannelId.CHANNEL_ID_BYTES + Long.BYTES + Integer.BYTES;
            result = allocateBuffer(allocator, MessageType.FETCH_SLICE_REQUEST.getId(), length);

            sliceId.writeTo(result);
            receiverId.writeTo(result);
            result.writeLong(startBatchId);
            result.writeInt(initialCredit);

            return result;
        } catch (Throwable t) {
            if (result != null) {
                result.release();
            }
            throw new IOException(t);
        }
    }

    public static SliceRequest readFrom(ByteBuf buffer) {
        SliceId sliceId = SliceId.readFrom(buffer);
        ChannelId receiverId = ChannelId.readFrom(buffer);
        long startBatchId = buffer.readLong();
        int initialCredits = buffer.readInt();

        return new SliceRequest(sliceId, startBatchId, receiverId, initialCredits);
    }

    @Override
    public String toString() {
        return String.format("SliceFetchRequest(%s, startBatchId=%s, initCredit=%s)", sliceId,
            startBatchId, initialCredit);
    }

}
