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

package org.apache.geaflow.state.descriptor;

import org.apache.geaflow.state.serializer.IKeySerializer;

public class KeyMapStateDescriptor<K, UK, UV> extends BaseKeyDescriptor<K> {

    private Class<UV> valueClazz;
    private Class<UK> subKeyClazz;

    protected KeyMapStateDescriptor(String name, String storeType) {
        super(name, storeType);
    }

    @Override
    public DescriptorType getDescriptorType() {
        return DescriptorType.KEY_MAP;
    }

    public static <K, UK, UV> KeyMapStateDescriptor<K, UK, UV> build(String name,
                                                                     String storeType) {
        return new KeyMapStateDescriptor<>(name, storeType);
    }

    public KeyMapStateDescriptor<K, UK, UV> withTypeInfo(Class<K> keyClazz, Class<UK> subKeyClazz, Class<UV> valueClazz) {
        this.keyClazz = keyClazz;
        this.subKeyClazz = subKeyClazz;
        this.valueClazz = valueClazz;
        return this;
    }

    public KeyMapStateDescriptor<K, UK, UV> withKeySerializer(IKeySerializer<K> kvSerializer) {
        this.keySerializer = kvSerializer;
        return this;
    }

    public Class<UV> getValueClazz() {
        return valueClazz;
    }

    public Class<UK> getSubKeyClazz() {
        return subKeyClazz;
    }
}
