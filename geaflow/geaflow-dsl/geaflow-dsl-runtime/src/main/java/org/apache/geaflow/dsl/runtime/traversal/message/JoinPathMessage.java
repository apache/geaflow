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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;

public class JoinPathMessage implements IPathMessage {

    private final Map<Long, ITreePath> senderId2Paths;

    public JoinPathMessage(Map<Long, ITreePath> senderId2Paths) {
        this.senderId2Paths = senderId2Paths;
    }

    public JoinPathMessage() {
        this(new HashMap<>());
    }

    public static JoinPathMessage from(long senderId, ITreePath treePath) {
        Map<Long, ITreePath> senderId2Paths = new HashMap<>();
        senderId2Paths.put(senderId, treePath);
        return new JoinPathMessage(senderId2Paths);
    }

    @Override
    public MessageType getType() {
        return MessageType.JOIN_PATH;
    }

    @Override
    public IMessage combine(IMessage other) {
        JoinPathMessage combinedTreePath = this.copy();
        JoinPathMessage otherTreePath = (JoinPathMessage) other;

        for (Map.Entry<Long, ITreePath> entry : otherTreePath.senderId2Paths.entrySet()) {
            long senderId = entry.getKey();
            ITreePath treePath = entry.getValue();
            if (combinedTreePath.senderId2Paths.containsKey(senderId)) {
                ITreePath mergeTree = combinedTreePath.senderId2Paths.get(senderId).merge(treePath);
                combinedTreePath.senderId2Paths.put(senderId, mergeTree);
            } else {
                combinedTreePath.senderId2Paths.put(senderId, treePath);
            }
        }
        return combinedTreePath;
    }

    @Override
    public JoinPathMessage copy() {
        return new JoinPathMessage(new HashMap<>(senderId2Paths));
    }

    @Override
    public IMessage getMessageByRequestId(Object requestId) {
        Map<Long, ITreePath> requestTreePaths = new HashMap<>(senderId2Paths.size());
        for (Map.Entry<Long, ITreePath> entry : senderId2Paths.entrySet()) {
            long senderId = entry.getKey();
            ITreePath treePath = (ITreePath) entry.getValue().getMessageByRequestId(requestId);
            requestTreePaths.put(senderId, treePath);
        }
        return new JoinPathMessage(requestTreePaths);
    }

    public ITreePath getTreePath(long senderId) {
        return senderId2Paths.get(senderId);
    }

    public boolean isEmpty() {
        return senderId2Paths.isEmpty();
    }

    public Set<Long> getSenders() {
        return senderId2Paths.keySet();
    }

    public static class JoinPathMessageSerializer extends Serializer<JoinPathMessage> {

        @Override
        public void write(Kryo kryo, Output output, JoinPathMessage object) {
            kryo.writeClassAndObject(output, object.senderId2Paths);
        }

        @Override
        public JoinPathMessage read(Kryo kryo, Input input, Class<JoinPathMessage> type) {
            Map<Long, ITreePath> senderId2Paths = (Map<Long, ITreePath>) kryo.readClassAndObject(input);
            return new JoinPathMessage(senderId2Paths);
        }

        @Override
        public JoinPathMessage copy(Kryo kryo, JoinPathMessage original) {
            return original.copy();
        }
    }
}
