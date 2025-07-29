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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.net.ProtocolException;
import java.util.List;

@ChannelHandler.Sharable
public class NettyMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
        throws Exception {
        int magicNumber = msg.readInt();

        if (magicNumber != NettyMessage.MAGIC_NUMBER) {
            throw new IllegalStateException(
                "Network stream corrupted: received incorrect magic number.");
        }

        byte msgId = msg.readByte();
        MessageType msgType = MessageType.decode(msgId);

        final NettyMessage decodedMsg;
        switch (msgType) {
            case ERROR_RESPONSE:
                decodedMsg = ErrorResponse.readFrom(msg);
                break;
            case FETCH_SLICE_REQUEST:
                decodedMsg = SliceRequest.readFrom(msg);
                break;
            case FETCH_SLICE_RESPONSE:
                decodedMsg = SliceResponse.readFrom(msg);
                break;
            case FETCH_BATCH_REQUEST:
                decodedMsg = BatchRequest.readFrom(msg);
                break;
            case CLOSE_CONNECTION:
                decodedMsg = CloseRequest.readFrom(msg);
                break;
            case CANCEL_CONNECTION:
                decodedMsg = CancelRequest.readFrom(msg);
                break;
            case ADD_CREDIT_REQUEST:
                decodedMsg = AddCreditRequest.readFrom(msg);
                break;
            default:
                throw new ProtocolException("Received unknown message from producer: " + msg);
        }

        out.add(decodedMsg);
    }
}
