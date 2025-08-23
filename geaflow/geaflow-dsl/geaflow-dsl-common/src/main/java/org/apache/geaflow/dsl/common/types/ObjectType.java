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

package org.apache.geaflow.dsl.common.types;

import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class ObjectType implements IType<Object> {

    public static ObjectType INSTANCE = new ObjectType();

    private ObjectType() {

    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_OBJECT;
    }

    @Override
    public Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    public byte[] serialize(Object obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        return SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @Override
    public int compare(Object x, Object y) {
        if (x instanceof Comparable && y instanceof Comparable) {
            return ((Comparable) x).compareTo(y);
        }
        return 0;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}
