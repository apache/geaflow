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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.dsl.runtime.traversal.data.SingleValue;

public class ReturnMessageImpl implements ReturnMessage {

    private final Map<ReturnKey, SingleValue> returnKey2Value;

    public ReturnMessageImpl(Map<ReturnKey, SingleValue> returnKey2Value) {
        this.returnKey2Value = returnKey2Value;
    }

    public ReturnMessageImpl() {
        this(new HashMap<>());
    }

    @Override
    public MessageType getType() {
        return MessageType.RETURN_VALUE;
    }

    @Override
    public ReturnMessage combine(IMessage other) {
        ReturnMessage copy = this.copy();
        ReturnMessage otherMsg = (ReturnMessage) other;

        for (Map.Entry<ReturnKey, SingleValue> entry : otherMsg.getReturnKey2Values().entrySet()) {
            ReturnKey returnKey = entry.getKey();
            SingleValue value = entry.getValue();
            if (value != null) {
                copy.putValue(returnKey, value);
            }
        }
        return copy;
    }

    @Override
    public ReturnMessage copy() {
        return new ReturnMessageImpl(new HashMap<>(returnKey2Value));
    }

    @Override
    public Map<ReturnKey, SingleValue> getReturnKey2Values() {
        return returnKey2Value;
    }

    @Override
    public void putValue(ReturnKey returnKey, SingleValue value) {
        returnKey2Value.put(returnKey, value);
    }

    @Override
    public SingleValue getValue(ReturnKey returnKey) {
        return returnKey2Value.get(returnKey);
    }


    public static class ReturnKey implements Serializable {

        private final long pathId;

        private final long queryId;

        public ReturnKey(long pathId, long queryId) {
            if (pathId < 0) {
                throw new IllegalArgumentException("Illegal pathId: " + pathId);
            }
            this.pathId = pathId;
            this.queryId = queryId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ReturnKey)) {
                return false;
            }
            ReturnKey returnKey = (ReturnKey) o;
            return queryId == returnKey.queryId && Objects.equals(pathId, returnKey.pathId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathId, queryId);
        }

        @Override
        public String toString() {
            return "ReturnKey{"
                + "pathId=" + pathId
                + ", queryId=" + queryId
                + '}';
        }
    }

    public static class ReturnMessageImplSerializer extends Serializer<ReturnMessageImpl> {

        @Override
        public void write(Kryo kryo, Output output, ReturnMessageImpl object) {
            kryo.writeClassAndObject(output, object.returnKey2Value);
        }

        @Override
        public ReturnMessageImpl read(Kryo kryo, Input input, Class<ReturnMessageImpl> type) {
            Map<ReturnMessageImpl.ReturnKey, SingleValue> returnKey2Value =
                (Map<ReturnMessageImpl.ReturnKey, SingleValue>) kryo.readClassAndObject(input);
            return new ReturnMessageImpl(returnKey2Value);
        }

        @Override
        public ReturnMessageImpl copy(Kryo kryo, ReturnMessageImpl original) {
            return new ReturnMessageImpl(new HashMap<>(original.returnKey2Value));
        }
    }

}
