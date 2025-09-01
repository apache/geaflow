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

package org.apache.geaflow.dsl.planner;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;

public class GQLJavaTypeFactory extends JavaTypeFactoryImpl {

    private GeaFlowGraph currentGraph;

    public static final String NATIVE_UTF16_CHARSET_NAME =
        (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? "UTF-16BE" : "UTF-16LE";

    public GQLJavaTypeFactory(RelDataTypeSystem relDataTypeSystem) {
        super(relDataTypeSystem);
    }

    public GeaFlowGraph getCurrentGraph() {
        return currentGraph;
    }

    public GeaFlowGraph setCurrentGraph(GeaFlowGraph graph) {
        GeaFlowGraph prevGraph = currentGraph;
        currentGraph = graph;
        return prevGraph;
    }

    @Override
    public Charset getDefaultCharset() {
        return Charset.forName(NATIVE_UTF16_CHARSET_NAME);
    }

    @Override
    public Type getJavaClass(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return BinaryString.class;
            case DATE:
                return Date.class;
            case TIME:
                return Time.class;
            case INTEGER:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case SYMBOL:
                return type.isNullable() ? Integer.class : int.class;
            case TIMESTAMP:
                return Timestamp.class;
            case BIGINT:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return type.isNullable() ? Long.class : long.class;
            case SMALLINT:
                return type.isNullable() ? Short.class : short.class;
            case TINYINT:
                return type.isNullable() ? Byte.class : byte.class;
            case DECIMAL:
                return BigDecimal.class;
            case BOOLEAN:
                return type.isNullable() ? Boolean.class : boolean.class;
            case DOUBLE:
            case FLOAT:
                return type.isNullable() ? Double.class : double.class;
            case REAL:
                return type.isNullable() ? Float.class : float.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case ARRAY:
                Class<?> elementType = (Class<?>) getJavaClass(type.getComponentType());
                try {
                    return Class.forName("[L" + elementType.getName() + ";");
                } catch (Exception e) {
                    throw new GeaFlowDSLException("Error in create Java Type for " + type, e);
                }
            case MAP:
                return Map.class;
            case VERTEX:
                return RowVertex.class;
            case EDGE:
                return RowEdge.class;
            case PATH:
                return Path.class;
            case ROW:
                return Row.class;
            default:
                return super.getJavaClass(type);
        }
    }

    @Override
    public RelDataType createType(Type type) {
        if (type instanceof Class && ((Class) type).isArray()) {
            Class<?> elementType = ((Class) type).getComponentType();
            return canonize(new ArraySqlType(createType(elementType), true));
        }
        if (type == BinaryString.class) {
            return super.createType(String.class);
        }
        return super.createType(type);
    }

    @Override
    public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable) {
        if (type.getSqlTypeName() == SqlTypeName.PATH || type instanceof MetaFieldType) {
            return type;
        }
        return super.createTypeWithNullability(type, nullable);
    }

    public static GQLJavaTypeFactory create() {
        return new GQLJavaTypeFactory(new GQLRelDataTypeSystem());
    }
}
