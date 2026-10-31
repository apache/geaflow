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

package org.apache.geaflow.dsl.connector.mongodb;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

class MongoRowConverter implements TableDeserializer<Document> {

    private StructType schema;

    MongoRowConverter(StructType schema) {
        this.schema = schema;
    }

    @Override
    public void init(Configuration conf, StructType structType) {
        this.schema = structType;
    }

    @Override
    public List<Row> deserialize(Document document) {
        return Collections.singletonList(toRow(document));
    }

    Row toRow(Document document) {
        Object[] values = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            TableField field = schema.getField(i);
            values[i] = fromBson(field.getName(), field.getType(), document.get(field.getName()));
        }
        return ObjectRow.create(values);
    }

    Document toDocument(Row row) {
        Document document = new Document();
        for (int i = 0; i < schema.size(); i++) {
            TableField field = schema.getField(i);
            Object value = row.getField(i, field.getType());
            document.put(field.getName(), toBson(field.getName(), field.getType(), value));
        }
        return document;
    }

    private Object fromBson(String field, IType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getName()) {
            case Types.TYPE_NAME_STRING:
                return asString(field, type, value);
            case Types.TYPE_NAME_BINARY_STRING:
                return BinaryString.fromString(asString(field, type, value));
            case Types.TYPE_NAME_BOOLEAN:
                return requireType(field, type, value, Boolean.class);
            case Types.TYPE_NAME_BYTE:
                return (byte) integralValue(field, type, value, Byte.MIN_VALUE, Byte.MAX_VALUE);
            case Types.TYPE_NAME_SHORT:
                return (short) integralValue(field, type, value, Short.MIN_VALUE, Short.MAX_VALUE);
            case Types.TYPE_NAME_INTEGER:
                return (int) integralValue(field, type, value, Integer.MIN_VALUE, Integer.MAX_VALUE);
            case Types.TYPE_NAME_LONG:
                return integralValue(field, type, value, Long.MIN_VALUE, Long.MAX_VALUE);
            case Types.TYPE_NAME_FLOAT:
                return floatValue(field, type, value);
            case Types.TYPE_NAME_DOUBLE:
                return decimalValue(field, type, value);
            case Types.TYPE_NAME_DECIMAL:
                return decimal128Value(field, type, value).bigDecimalValue();
            case Types.TYPE_NAME_TIMESTAMP:
                return new Timestamp(dateValue(field, type, value).getTime());
            case Types.TYPE_NAME_DATE:
                return new java.sql.Date(dateValue(field, type, value).getTime());
            default:
                throw conversionError(field, type, value);
        }
    }

    private Object toBson(String field, IType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getName()) {
            case Types.TYPE_NAME_STRING:
                return requireType(field, type, value, String.class);
            case Types.TYPE_NAME_BINARY_STRING:
                return requireType(field, type, value, BinaryString.class).toString();
            case Types.TYPE_NAME_BOOLEAN:
                return requireType(field, type, value, Boolean.class);
            case Types.TYPE_NAME_BYTE:
                return (int) requireType(field, type, value, Byte.class);
            case Types.TYPE_NAME_SHORT:
                return (int) requireType(field, type, value, Short.class);
            case Types.TYPE_NAME_INTEGER:
                return requireType(field, type, value, Integer.class);
            case Types.TYPE_NAME_LONG:
                return requireType(field, type, value, Long.class);
            case Types.TYPE_NAME_FLOAT:
                return (double) requireType(field, type, value, Float.class);
            case Types.TYPE_NAME_DOUBLE:
                return requireType(field, type, value, Double.class);
            case Types.TYPE_NAME_DECIMAL:
                return new Decimal128(requireType(field, type, value, BigDecimal.class));
            case Types.TYPE_NAME_TIMESTAMP:
                Timestamp timestamp = requireType(field, type, value, Timestamp.class);
                return new java.util.Date(timestamp.getTime());
            case Types.TYPE_NAME_DATE:
                java.sql.Date date = requireType(field, type, value, java.sql.Date.class);
                return new java.util.Date(date.getTime());
            default:
                throw conversionError(field, type, value);
        }
    }

    private String asString(String field, IType<?> type, Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        if ("_id".equals(field) && value instanceof ObjectId) {
            return ((ObjectId) value).toHexString();
        }
        throw conversionError(field, type, value);
    }

    private long integralValue(String field, IType<?> type, Object value, long min, long max) {
        if (!(value instanceof Number)) {
            throw conversionError(field, type, value);
        }
        Number number = (Number) value;
        try {
            BigDecimal decimal;
            if (number instanceof Decimal128) {
                decimal = ((Decimal128) number).bigDecimalValue();
            } else if (number instanceof BigDecimal) {
                decimal = (BigDecimal) number;
            } else if (number instanceof Byte || number instanceof Short
                || number instanceof Integer || number instanceof Long) {
                decimal = BigDecimal.valueOf(number.longValue());
            } else {
                double doubleValue = number.doubleValue();
                if (!Double.isFinite(doubleValue)) {
                    throw conversionError(field, type, value);
                }
                decimal = BigDecimal.valueOf(doubleValue);
            }
            long longValue = decimal.longValueExact();
            if (longValue < min || longValue > max) {
                throw conversionError(field, type, value);
            }
            return longValue;
        } catch (ArithmeticException e) {
            throw conversionError(field, type, value);
        }
    }

    private float floatValue(String field, IType<?> type, Object value) {
        double result = decimalValue(field, type, value);
        float floatResult = (float) result;
        if (!Float.isFinite(floatResult)) {
            throw conversionError(field, type, value);
        }
        return floatResult;
    }

    private double decimalValue(String field, IType<?> type, Object value) {
        if (!(value instanceof Number)) {
            throw conversionError(field, type, value);
        }
        double result = ((Number) value).doubleValue();
        if (!Double.isFinite(result)) {
            throw conversionError(field, type, value);
        }
        return result;
    }

    private Decimal128 decimal128Value(String field, IType<?> type, Object value) {
        if (value instanceof Decimal128) {
            return (Decimal128) value;
        }
        if (value instanceof BigDecimal) {
            return new Decimal128((BigDecimal) value);
        }
        throw conversionError(field, type, value);
    }

    private java.util.Date dateValue(String field, IType<?> type, Object value) {
        if (!(value instanceof java.util.Date)) {
            throw conversionError(field, type, value);
        }
        return (java.util.Date) value;
    }

    private <T> T requireType(String field, IType<?> type, Object value, Class<T> valueClass) {
        if (!valueClass.isInstance(value)) {
            throw conversionError(field, type, value);
        }
        return valueClass.cast(value);
    }

    private GeaFlowDSLException conversionError(String field, IType<?> type, Object value) {
        return new GeaFlowDSLException("Cannot map MongoDB field '{}' from {} to {}", field,
            value.getClass().getSimpleName(), type.getName());
    }
}
