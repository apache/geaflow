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
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;

public class AddCreditRequest extends NettyMessage {

    private final ChannelId receiverId;
    private final int credit;

    public AddCreditRequest(int credit, ChannelId receiverId) {
        this.receiverId = receiverId;
        this.credit = credit;
    }

    public ChannelId receiverId() {
        return receiverId;
    }

    public int getCredit() {
        return credit;
    }

    @Override
    public Object write(ByteBufAllocator allocator) throws Exception {
        ByteBuf result = null;

        try {
            int length = Integer.BYTES + ChannelId.CHANNEL_ID_BYTES;
            result = allocateBuffer(allocator, MessageType.ADD_CREDIT_REQUEST.getId(), length);
            result.writeInt(credit);
            receiverId.writeTo(result);

            return result;
        } catch (Throwable t) {
            if (result != null) {
                result.release();
            }

            throw new IOException(t);
        }
    }

    public static AddCreditRequest readFrom(ByteBuf buffer) {
        int credit = buffer.readInt();
        ChannelId receiverId = ChannelId.readFrom(buffer);

        return new AddCreditRequest(credit, receiverId);
    }

    @Override
    public String toString() {
        return String.format("AddCredit(%s: %d)", receiverId, credit);
    }
}
