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

import java.lang.reflect.Array;
import java.util.Objects;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class ArrayType implements IType<Object[]> {

    private final IType<?> componentType;

    public ArrayType(IType<?> componentType) {
        this.componentType = Objects.requireNonNull(componentType);
    }

    public IType<?> getComponentType() {
        return componentType;
    }

    @Override
    public String toString() {
        return "Array<" + componentType + ">";
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_ARRAY;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Object[]> getTypeClass() {
        return (Class<Object[]>) Array.newInstance(componentType.getTypeClass(), 0).getClass();
    }

    @Override
    public byte[] serialize(Object[] obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public Object[] deserialize(byte[] bytes) {
        return (Object[]) SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object[] x, Object[] y) {
        if (null == x) {
            return y == null ? 0 : -1;
        } else if (y == null) {
            return 1;
        }
        int i;
        for (i = 0; i < x.length && i < y.length; i++) {
            Comparable<Object> cx = (Comparable<Object>) x[i];
            Comparable<Object> cy = (Comparable<Object>) y[i];
            if (cx == null && cy != null) {
                return -1;
            }
            if (cx != null && cy == null) {
                return 1;
            }
            if (cx == cy) {
                return 0;
            }
            int c = cx.compareTo(cy);
            if (c != 0) {
                return c;
            }
        }
        if (x.length > i) {
            return 1;
        }
        if (y.length > i) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ArrayType)) {
            return false;
        }
        ArrayType arrayType = (ArrayType) o;
        return Objects.equals(componentType, arrayType.componentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentType);
    }
}
