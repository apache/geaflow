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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;

public class ErrorResponse extends NettyMessage {

    private ChannelId channelId;
    private final Throwable cause;

    public ErrorResponse(ChannelId channelId, Throwable cause) {
        this.channelId = channelId;
        this.cause = Preconditions.checkNotNull(cause);
    }

    public ErrorResponse(Throwable cause) {
        this.cause = Preconditions.checkNotNull(cause);
    }

    @Override
    public ByteBuf write(ByteBufAllocator allocator) throws IOException {
        final ByteBuf result = allocateBuffer(allocator, MessageType.ERROR_RESPONSE.getId());
        channelId.writeTo(result);

        try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
            oos.writeObject(cause);
            result.setInt(0, result.readableBytes());
            return result;
        } catch (Throwable t) {
            result.release();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    public static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
        ChannelId channelId = ChannelId.readFrom(buffer);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
            Object obj = ois.readObject();

            if (!(obj instanceof Throwable)) {
                throw new ClassCastException(
                    "Read object expected to be of type Throwable, " + "actual type is " + obj
                        .getClass());
            }
            return new ErrorResponse(channelId, (Throwable) obj);
        }
    }

    public ChannelId getChannelId() {
        return channelId;
    }

    public Throwable getCause() {
        return cause;
    }

    public boolean isFatalError() {
        return channelId == null;
    }

}
