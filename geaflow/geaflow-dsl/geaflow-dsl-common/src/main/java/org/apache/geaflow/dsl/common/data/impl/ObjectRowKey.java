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

package org.apache.geaflow.dsl.common.data.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.RowKey;

public class ObjectRowKey implements RowKey {

    private final Object[] keys;

    private ObjectRowKey(Object... keys) {
        this.keys = Objects.requireNonNull(keys);
    }

    public static RowKey of(Object... keys) {
        return new ObjectRowKey(keys);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return keys[i];
    }

    @Override
    public Object[] getKeys() {
        return keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectRowKey)) {
            return false;
        }
        ObjectRowKey that = (ObjectRowKey) o;
        return Arrays.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keys);
    }

    @Override
    public String toString() {
        return "ObjectRowKey{"
            + "keys=" + Arrays.toString(keys)
            + '}';
    }

    public static class ObjectRowKeySerializer extends Serializer<ObjectRowKey> {

        @Override
        public void write(Kryo kryo, Output output, ObjectRowKey objectRowKey) {
            output.writeInt(objectRowKey.getKeys().length);
            for (Object key : objectRowKey.getKeys()) {
                kryo.writeClassAndObject(output, key);
            }
        }

        @Override
        public ObjectRowKey read(Kryo kryo, Input input, Class<ObjectRowKey> aClass) {
            int size = input.readInt();
            Object[] keys = new Object[size];
            for (int i = 0; i < size; i++) {
                keys[i] = kryo.readClassAndObject(input);
            }
            return new ObjectRowKey(keys);
        }

        @Override
        public ObjectRowKey copy(Kryo kryo, ObjectRowKey original) {
            return new ObjectRowKey(original.getKeys());
        }
    }
}
