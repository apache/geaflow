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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;

public class EODMessage implements IMessage {

    private final List<EndOfData> endOfDatas;

    private EODMessage() {
        this(new ArrayList<>());
    }

    private EODMessage(List<EndOfData> endOfDatas) {
        this.endOfDatas = Objects.requireNonNull(endOfDatas);
    }

    public static EODMessage of(EndOfData data) {
        return new EODMessage(Lists.newArrayList(data));
    }

    @Override
    public MessageType getType() {
        return MessageType.EOD;
    }

    @Override
    public IMessage combine(IMessage other) {
        EODMessage newMessage = this.copy();
        EODMessage otherEod = (EODMessage) other;
        newMessage.endOfDatas.addAll(otherEod.endOfDatas);
        return newMessage;
    }

    @Override
    public EODMessage copy() {
        return new EODMessage(new ArrayList<>(endOfDatas));
    }

    public List<EndOfData> getEodData() {
        return endOfDatas;
    }

    public static class EODMessageSerializer extends Serializer<EODMessage> {

        @Override
        public void write(Kryo kryo, Output output, EODMessage object) {
            // serialize endOfDatas using default CollectionSerializer
            kryo.writeObject(output, object.getEodData());
        }

        @Override
        public EODMessage read(Kryo kryo, Input input, Class<EODMessage> type) {
            // deserialize endOfDatas using default CollectionSerializer
            List<EndOfData> endOfDatas = kryo.readObject(input, ArrayList.class);
            return new EODMessage(endOfDatas);
        }

        @Override
        public EODMessage copy(Kryo kryo, EODMessage original) {
            return new EODMessage(new ArrayList<>(original.getEodData()));
        }
    }

}
