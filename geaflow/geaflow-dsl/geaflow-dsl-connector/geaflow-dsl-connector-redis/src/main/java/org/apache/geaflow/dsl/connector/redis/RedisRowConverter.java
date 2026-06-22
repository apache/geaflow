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

package org.apache.geaflow.dsl.connector.redis;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public class RedisRowConverter {

    public static String toRedisString(Object value) {
        return value == null ? null : value.toString();
    }

    public static Object fromRedisString(String value, IType<?> type) {
        if (value == null) {
            return null;
        }
        String typeName = type.getName();
        switch (typeName) {
            case Types.TYPE_NAME_STRING:
                return value;
            case Types.TYPE_NAME_BINARY_STRING:
                return BinaryString.fromString(value);
            case Types.TYPE_NAME_BOOLEAN:
                return Boolean.valueOf(value);
            case Types.TYPE_NAME_BYTE:
                return Byte.valueOf(value);
            case Types.TYPE_NAME_SHORT:
                return Short.valueOf(value);
            case Types.TYPE_NAME_INTEGER:
                return Integer.valueOf(value);
            case Types.TYPE_NAME_LONG:
                return Long.valueOf(value);
            case Types.TYPE_NAME_FLOAT:
                return Float.valueOf(value);
            case Types.TYPE_NAME_DOUBLE:
                return Double.valueOf(value);
            case Types.TYPE_NAME_DECIMAL:
                return new BigDecimal(value);
            case Types.TYPE_NAME_TIMESTAMP:
                return Timestamp.valueOf(value);
            case Types.TYPE_NAME_DATE:
                return Date.valueOf(value);
            default:
                throw new GeaFlowDSLException("Redis connector does not support type: " + typeName);
        }
    }
}
