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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.common.data.Row;

public class KeyGroupMessageImpl implements KeyGroupMessage {

    private final List<Row> groupRows;

    public KeyGroupMessageImpl(List<Row> groupRows) {
        this.groupRows = Objects.requireNonNull(groupRows);
    }

    @Override
    public MessageType getType() {
        return MessageType.KEY_GROUP;
    }

    @Override
    public IMessage combine(IMessage other) {
        List<Row> combineRows = new ArrayList<>(groupRows);
        KeyGroupMessage groupMessage = (KeyGroupMessage) other;
        combineRows.addAll(groupMessage.getGroupRows());
        return new KeyGroupMessageImpl(combineRows);
    }

    @Override
    public KeyGroupMessage copy() {
        return new KeyGroupMessageImpl(new ArrayList<>(groupRows));
    }

    @Override
    public List<Row> getGroupRows() {
        return groupRows;
    }

    public static class KeyGroupMessageImplSerializer extends Serializer<KeyGroupMessageImpl> {

        @Override
        public void write(Kryo kryo, Output output, KeyGroupMessageImpl object) {
            kryo.writeObject(output, object.groupRows);
        }

        @Override
        public KeyGroupMessageImpl read(Kryo kryo, Input input, Class<KeyGroupMessageImpl> type) {
            List<Row> groupRows = kryo.readObject(input, ArrayList.class);
            return new KeyGroupMessageImpl(groupRows);
        }

        @Override
        public KeyGroupMessageImpl copy(Kryo kryo, KeyGroupMessageImpl original) {
            return new KeyGroupMessageImpl(new ArrayList<>(original.getGroupRows()));
        }
    }

}
