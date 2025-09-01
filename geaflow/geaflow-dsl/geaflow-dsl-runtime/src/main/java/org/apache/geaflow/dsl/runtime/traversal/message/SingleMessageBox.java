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

package org.apache.geaflow.dsl.runtime.traversal.message;

import java.util.Arrays;
import org.apache.geaflow.common.utils.ArrayUtil;

public abstract class SingleMessageBox implements MessageBox {

    // The operator id who receive the message.
    protected long[] receiverIds;

    // Messages for each receiver.
    protected IMessage[] messages;

    @Override
    public void addMessage(long receiverId, IMessage message) {
        int index;
        IMessage oldMessage = null;
        if (receiverIds == null) {
            receiverIds = new long[1];
            messages = new IMessage[1];
            index = 0;
        } else {
            index = ArrayUtil.indexOf(receiverIds, receiverId);
            if (index == -1) {
                index = receiverIds.length;
                receiverIds = ArrayUtil.grow(receiverIds, 1);
                IMessage[] newMessages = new IMessage[messages.length + 1];
                System.arraycopy(messages, 0, newMessages, 0, messages.length);
                messages = newMessages;
            } else {
                oldMessage = messages[index];
            }
        }
        receiverIds[index] = receiverId;
        if (oldMessage == null) {
            // Copy the first message as the combine() method will modify the old message.
            messages[index] = message.copy();
        } else {
            messages[index] = oldMessage.combine(message);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends IMessage> M getMessage(long receiverId, MessageType messageType) {
        if (messageType != getMessageType()) {
            return null;
        }
        int index = ArrayUtil.indexOf(receiverIds, receiverId);
        if (index == -1) {
            return null;
        }
        return (M) messages[index];
    }

    @Override
    public long[] getReceiverIds() {
        return receiverIds;
    }

    public abstract MessageType getMessageType();

    @Override
    public MessageBox combine(MessageBox other) {
        if (other.isEmpty()) {
            return this;
        }
        if (other instanceof SingleMessageBox && canMerge((SingleMessageBox) other)) {
            MessageBox newBox = this.copy();
            SingleMessageBox otherBox = (SingleMessageBox) other;
            for (long receiverId : otherBox.getReceiverIds()) {
                IMessage message = otherBox.getMessage(receiverId, getMessageType());
                newBox.addMessage(receiverId, message);
            }
            return newBox;
        } else {
            UnionMessageBox unionBox = new UnionMessageBox();
            unionBox.addMessageBox(this);
            unionBox.addMessageBox(other);
            return unionBox;
        }
    }

    protected boolean canMerge(SingleMessageBox other) {
        return other.getMessageType() == this.getMessageType();
    }

    @Override
    public MessageBox copy() {
        SingleMessageBox newMessage = create();
        if (receiverIds != null) {
            newMessage.receiverIds = ArrayUtil.copy(receiverIds);
        }
        if (messages != null) {
            newMessage.messages = Arrays.copyOf(messages, messages.length);
        }
        return newMessage;
    }

    protected abstract SingleMessageBox create();
}
