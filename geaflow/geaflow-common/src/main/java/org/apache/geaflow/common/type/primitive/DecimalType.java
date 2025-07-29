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

package org.apache.geaflow.common.type.primitive;

import java.math.BigDecimal;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class DecimalType implements IType<BigDecimal> {

    public static final DecimalType INSTANCE = new DecimalType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_DECIMAL;
    }

    @Override
    public Class<BigDecimal> getTypeClass() {
        return BigDecimal.class;
    }

    @Override
    public byte[] serialize(BigDecimal obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public BigDecimal deserialize(byte[] bytes) {
        return (BigDecimal) SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @Override
    public int compare(BigDecimal a, BigDecimal b) {
        return Types.compare(a, b);
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DecimalType decimal = (DecimalType) o;
        return this.getName().equals(decimal.getName());
    }
}
