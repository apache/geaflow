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

public class DefaultKMapSerializer<K, UK, UV> implements IKMapSerializer<K, UK, UV> {

    private Function<K, byte[]> keySerializer;
    private Function<byte[], K> keyDeserializer;
    private Function<UK, byte[]> subKeySerializer;
    private Function<byte[], UK> subKeyDeserializer;
    private Function<UV, byte[]> valueSerializer;
    private Function<byte[], UV> valueDeserializer;

    public DefaultKMapSerializer(Class<K> keyClazz, Class<UK> subKeyClazz, Class<UV> valueClazz) {
        final IType<K> keyType = Types.getType(keyClazz);
        final IType<UK> subKeyType = Types.getType(subKeyClazz);
        final IType<UV> valueType = Types.getType(valueClazz);
        ISerializer serializer = SerializerFactory.getKryoSerializer();

        keySerializer = keyType == null ? serializer::serialize : keyType::serialize;
        keyDeserializer = keyType == null ? c -> (K) serializer.deserialize(c) : keyType::deserialize;
        subKeySerializer = subKeyType == null ? serializer::serialize : subKeyType::serialize;
        subKeyDeserializer = subKeyType == null ? c -> (UK) serializer.deserialize(c) : subKeyType::deserialize;
        valueSerializer = valueType == null ? serializer::serialize : valueType::serialize;
        valueDeserializer = valueType == null ? c -> (UV) serializer.deserialize(c) : valueType::deserialize;
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
    public byte[] serializeUK(UK key) {
        return subKeySerializer.apply(key);
    }

    @Override
    public UK deserializeUK(byte[] array) {
        return subKeyDeserializer.apply(array);
    }

    @Override
    public byte[] serializeUV(UV value) {
        return valueSerializer.apply(value);
    }

    @Override
    public UV deserializeUV(byte[] valueArray) {
        return valueDeserializer.apply(valueArray);
    }
}
