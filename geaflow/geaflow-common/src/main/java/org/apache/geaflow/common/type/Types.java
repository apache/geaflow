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

package org.apache.geaflow.common.type;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Locale;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.ByteType;
import org.apache.geaflow.common.type.primitive.DateType;
import org.apache.geaflow.common.type.primitive.DecimalType;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.FloatType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.ShortType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.common.type.primitive.TimestampType;

public class Types {

    public static final String TYPE_NAME_BOOLEAN = "BOOLEAN";
    public static final String TYPE_NAME_BYTE = "BYTE";
    public static final String TYPE_NAME_SHORT = "SHORT";
    public static final String TYPE_NAME_INTEGER = "INTEGER";
    public static final String TYPE_NAME_LONG = "LONG";
    public static final String TYPE_NAME_FLOAT = "FLOAT";
    public static final String TYPE_NAME_DOUBLE = "DOUBLE";
    public static final String TYPE_NAME_STRING = "STRING";
    public static final String TYPE_NAME_STRUCT = "STRUCT";
    public static final String TYPE_NAME_ARRAY = "ARRAY";
    public static final String TYPE_NAME_VERTEX = "VERTEX";
    public static final String TYPE_NAME_EDGE = "EDGE";
    public static final String TYPE_NAME_PATH = "PATH";
    public static final String TYPE_NAME_CLASS = "CLASS";
    public static final String TYPE_NAME_DECIMAL = "DECIMAL";
    public static final String TYPE_NAME_GRAPH = "GRAPH";
    public static final String TYPE_NAME_OBJECT = "OBJECT";
    public static final String TYPE_NAME_BINARY_STRING = "BINARY_STRING";
    public static final String TYPE_NAME_TIMESTAMP = "TIMESTAMP";
    public static final String TYPE_NAME_DATE = "DATE";

    public static final IType<Boolean> BOOLEAN = BooleanType.INSTANCE;
    public static final IType<Byte> BYTE = ByteType.INSTANCE;
    public static final IType<Short> SHORT = ShortType.INSTANCE;
    public static final IType<Integer> INTEGER = IntegerType.INSTANCE;
    public static final IType<Long> LONG = LongType.INSTANCE;
    public static final IType<Float> FLOAT = FloatType.INSTANCE;
    public static final IType<Double> DOUBLE = DoubleType.INSTANCE;
    public static final IType<String> STRING = StringType.INSTANCE;
    public static final IType<BigDecimal> DECIMAL = DecimalType.INSTANCE;
    public static final IType<BinaryString> BINARY_STRING = BinaryStringType.INSTANCE;
    public static final IType<Timestamp> TIMESTAMP = TimestampType.INSTANCE;
    public static final IType<Date> DATE = DateType.INSTANCE;

    public static final ImmutableMap<Class, IType> TYPE_IMMUTABLE_MAP =
        ImmutableMap.<Class, IType>builder()
            .put(BOOLEAN.getTypeClass(), BOOLEAN)
            .put(BYTE.getTypeClass(), BYTE)
            .put(SHORT.getTypeClass(), SHORT)
            .put(INTEGER.getTypeClass(), INTEGER)
            .put(LONG.getTypeClass(), LONG)
            .put(FLOAT.getTypeClass(), FLOAT)
            .put(DOUBLE.getTypeClass(), DOUBLE)
            .put(STRING.getTypeClass(), STRING)
            .put(DECIMAL.getTypeClass(), DECIMAL)
            .put(BINARY_STRING.getTypeClass(), BINARY_STRING)
            .put(TIMESTAMP.getTypeClass(), TIMESTAMP)
            .put(DATE.getTypeClass(), DATE)
            .build();

    public static <T> IType<T> getType(Class<T> type) {
        return TYPE_IMMUTABLE_MAP.get(type);
    }

    public static IType<?> of(String typeName, int precision) {
        if (typeName == null) {
            throw new IllegalArgumentException("typeName is null");
        }
        switch (typeName.toUpperCase(Locale.ROOT)) {
            case TYPE_NAME_BOOLEAN:
                return BOOLEAN;
            case TYPE_NAME_BYTE:
                return BYTE;
            case TYPE_NAME_DOUBLE:
                return DOUBLE;
            case TYPE_NAME_FLOAT:
                return FLOAT;
            case TYPE_NAME_INTEGER:
                return INTEGER;
            case TYPE_NAME_LONG:
                return LONG;
            case TYPE_NAME_STRING:
                return STRING;
            case TYPE_NAME_DECIMAL:
                return DECIMAL;
            case TYPE_NAME_BINARY_STRING:
                return new BinaryStringType(precision);
            case TYPE_NAME_TIMESTAMP:
                return TIMESTAMP;
            case TYPE_NAME_DATE:
                return DATE;
            default:
                throw new IllegalArgumentException("Not support typeName: " + typeName);
        }
    }

    public static int compare(Comparable a, Comparable b) {
        if (null == a) {
            return b == null ? 0 : -1;
        } else if (b == null) {
            return 1;
        } else {
            return a.compareTo(b);
        }
    }
}
