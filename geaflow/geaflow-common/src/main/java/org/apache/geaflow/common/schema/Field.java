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

package org.apache.geaflow.common.schema;

import java.io.Serializable;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class Field implements Serializable {

    private final String name;
    private final IType<?> type;
    private final boolean nullable;
    protected final Object defaultValue;

    public Field(String name, IType<?> type) {
        this(name, type, true, null);
    }

    public Field(String name, IType<?> type, boolean nullable, Object defaultValue) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return this.name;
    }

    public IType<?> getType() {
        return this.type;
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(this.name, field.name) && this.type == field.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "Field{"
            + "name='" + name + '\''
            + ", type=" + type
            + ", nullable=" + nullable
            + ", defaultValue=" + defaultValue
            + '}';
    }

    public static class ByteField extends Field {

        public ByteField(String name, boolean nullable, Byte defaultValue) {
            super(name, Types.BYTE, nullable, defaultValue);
        }

        @Override
        public Byte getDefaultValue() {
            return this.defaultValue == null ? null : (Byte) this.defaultValue;
        }

    }

    public static class ShortField extends Field {

        public ShortField(String name, boolean nullable, Short defaultValue) {
            super(name, Types.SHORT, nullable, defaultValue);
        }

        @Override
        public Short getDefaultValue() {
            return this.defaultValue == null ? null : (Short) this.defaultValue;
        }

    }

    public static class IntegerField extends Field {

        public IntegerField(String name, boolean nullable, Integer defaultValue) {
            super(name, Types.INTEGER, nullable, defaultValue);
        }

        @Override
        public Integer getDefaultValue() {
            return this.defaultValue == null ? null : (Integer) this.defaultValue;
        }

    }

    public static class LongField extends Field {

        public LongField(String name, boolean nullable, Long defaultValue) {
            super(name, Types.LONG, nullable, defaultValue);
        }

        @Override
        public Long getDefaultValue() {
            return this.defaultValue == null ? null : (Long) this.defaultValue;
        }

    }

    public static class BooleanField extends Field {

        public BooleanField(String name, boolean nullable, Boolean defaultValue) {
            super(name, Types.BOOLEAN, nullable, defaultValue);
        }

        @Override
        public Boolean getDefaultValue() {
            return this.defaultValue == null ? null : (Boolean) this.defaultValue;
        }

    }

    public static class FloatField extends Field {

        public FloatField(String name, boolean nullable, Float defaultValue) {
            super(name, Types.FLOAT, nullable, defaultValue);
        }

        @Override
        public Float getDefaultValue() {
            return this.defaultValue == null ? null : (Float) this.defaultValue;
        }

    }

    public static class DoubleField extends Field {

        public DoubleField(String name, boolean nullable, Double defaultValue) {
            super(name, Types.DOUBLE, nullable, defaultValue);
        }

        @Override
        public Double getDefaultValue() {
            return this.defaultValue == null ? null : (Double) this.defaultValue;
        }

    }

    public static class StringField extends Field {

        public StringField(String name, boolean nullable, String defaultValue) {
            super(name, Types.STRING, nullable, defaultValue);
        }

        @Override
        public String getDefaultValue() {
            return this.defaultValue == null ? null : this.defaultValue.toString();
        }
    }

    public static ByteField newByteField(String fieldName) {
        return new ByteField(fieldName, true, null);
    }

    public static ByteField newByteField(String fieldName, boolean nullable, Byte defaultValue) {
        return new ByteField(fieldName, nullable, defaultValue);
    }

    public static ShortField newShortField(String fieldName) {
        return new ShortField(fieldName, true, null);
    }

    public static ShortField newShortField(String fieldName, boolean nullable, Short defaultValue) {
        return new ShortField(fieldName, nullable, defaultValue);
    }

    public static IntegerField newIntegerField(String fieldName) {
        return new IntegerField(fieldName, true, null);
    }

    public static IntegerField newIntegerField(String fieldName, boolean nullable, Integer defaultValue) {
        return new IntegerField(fieldName, nullable, defaultValue);
    }

    public static LongField newLongField(String fieldName) {
        return new LongField(fieldName, true, null);
    }

    public static LongField newLongField(String fieldName, boolean nullable, Long defaultValue) {
        return new LongField(fieldName, nullable, defaultValue);
    }

    public static BooleanField newBooleanField(String fieldName) {
        return new BooleanField(fieldName, true, null);
    }

    public static BooleanField newBooleanField(String fieldName, boolean nullable, Boolean defaultValue) {
        return new BooleanField(fieldName, nullable, defaultValue);
    }

    public static FloatField newFloatField(String fieldName) {
        return new FloatField(fieldName, true, null);
    }

    public static FloatField newFloatField(String fieldName, boolean nullable, Float defaultValue) {
        return new FloatField(fieldName, nullable, defaultValue);
    }

    public static DoubleField newDoubleField(String fieldName) {
        return new DoubleField(fieldName, true, null);
    }

    public static DoubleField newDoubleField(String fieldName, boolean nullable, Double defaultValue) {
        return new DoubleField(fieldName, nullable, defaultValue);
    }

    public static StringField newStringField(String fieldName) {
        return new StringField(fieldName, true, null);
    }

    public static StringField newStringField(String fieldName, boolean nullable, String defaultValue) {
        return new StringField(fieldName, nullable, defaultValue);
    }

}
