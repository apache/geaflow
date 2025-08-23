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

package org.apache.geaflow.state.serializer;

import java.util.function.Function;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class DefaultKVSerializer<K, V> implements IKVSerializer<K, V> {

    private Function<K, byte[]> keySerializer;
    private Function<V, byte[]> valueSerializer;
    private Function<byte[], K> keyDeserializer;
    private Function<byte[], V> valueDeserializer;

    public DefaultKVSerializer(Class<K> keyClazz, Class<V> valueClazz) {
        IType<K> keyType = Types.getType(keyClazz);
        IType<V> valueType = Types.getType(valueClazz);
        ISerializer serializer = SerializerFactory.getKryoSerializer();

        keySerializer = keyType == null ? serializer::serialize : keyType::serialize;
        keyDeserializer = keyType == null ? c -> (K) serializer.deserialize(c) : keyType::deserialize;
        valueSerializer = valueType == null ? serializer::serialize : valueType::serialize;
        valueDeserializer = valueType == null ? c -> (V) serializer.deserialize(c) : valueType::deserialize;
    }

    @Override
    public byte[] serializeKey(K key) {
        return keySerializer.apply(key);
    }

    @Override
    public K deserializeKey(byte[] array) {
        return keyDeserializer.apply(array);
    }

    @Override
    public byte[] serializeValue(V value) {
        return valueSerializer.apply(value);
    }

    @Override
    public V deserializeValue(byte[] valueArray) {
        return valueDeserializer.apply(valueArray);
    }
}
