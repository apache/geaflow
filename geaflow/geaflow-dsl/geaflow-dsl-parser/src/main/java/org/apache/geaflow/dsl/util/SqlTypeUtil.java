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

package org.apache.geaflow.dsl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;

public final class SqlTypeUtil {

    public static IType<?> convertType(SqlDataTypeSpec typeSpec) {
        String typeName = typeSpec.getTypeName().getSimple().toUpperCase();
        typeName = convertTypeName(typeName);
        return Types.of(typeName, typeSpec.getPrecision());
    }

    public static IType<?> convertType(RelDataType type) {
        SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch (sqlTypeName) {
            case ARRAY:
                RelDataType componentType = type.getComponentType();
                return new ArrayType(convertType(componentType));
            case STRUCTURED:
            case ROW:
                List<TableField> fields = toTableFields(type.getFieldList());
                return new StructType(fields);
            case VERTEX:
                VertexRecordType vertexType = (VertexRecordType) type;
                List<TableField> vertexFields = toTableFields(vertexType.getFieldList());
                return new VertexType(vertexFields);
            case EDGE:
                EdgeRecordType edgeType = (EdgeRecordType) type;
                List<TableField> edgeFields = toTableFields(edgeType.getFieldList());
                return new EdgeType(edgeFields, edgeType.getTimestampField().isPresent());
            case PATH:
                PathRecordType pathType = (PathRecordType) type;
                List<TableField> pathFields = toTableFields(pathType.getFieldList());
                return new PathType(pathFields);
            case GRAPH:
                GraphRecordType graphType = (GraphRecordType) type;
                List<TableField> graphFields = toTableFields(graphType.getFieldList());
                return new GraphSchema(graphType.getGraphName(), graphFields);
            default:
                return ofTypeName(sqlTypeName);
        }
    }

    private static List<TableField> toTableFields(List<RelDataTypeField> fields) {
        return fields.stream().map(field ->
                new TableField(field.getName(), convertType(field.getType()),
                    field.getType().isNullable()))
            .collect(Collectors.toList());
    }

    public static IType<?> ofTypeName(SqlTypeName sqlTypeName) {
        String typeName = convertTypeName(sqlTypeName.getName());
        return Types.of(typeName, sqlTypeName.getPrecision());
    }

    public static RelDataType convertToRelType(IType<?> type, boolean isNullable,
                                               RelDataTypeFactory typeFactory) {
        switch (type.getName()) {
            case Types.TYPE_NAME_ARRAY:
                ArrayType arrayType = (ArrayType) type;
                RelDataType componentType = convertToRelType(arrayType.getComponentType(), isNullable, typeFactory);
                return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(componentType, -1), true);
            case Types.TYPE_NAME_STRUCT:
                StructType structType = (StructType) type;
                List<RelDataTypeField> fields = toRecordFields(structType.getFields(), typeFactory);
                return new RelRecordType(StructKind.PEEK_FIELDS, fields);
            case Types.TYPE_NAME_VERTEX:
                VertexType vertexType = (VertexType) type;
                List<RelDataTypeField> vertexFields = toRecordFields(vertexType.getFields(), typeFactory);
                return VertexRecordType.createVertexType(vertexFields,
                    vertexType.getId().getName(), typeFactory);
            case Types.TYPE_NAME_EDGE:
                EdgeType edgeType = (EdgeType) type;
                List<RelDataTypeField> edgeFields = toRecordFields(edgeType.getFields(), typeFactory);
                return EdgeRecordType.createEdgeType(edgeFields,
                    edgeType.getSrcId().getName(),
                    edgeType.getTargetId().getName(),
                    edgeType.getTimestamp().map(TableField::getName).orElse(null),
                    typeFactory);
            case Types.TYPE_NAME_PATH:
                PathType pathType = (PathType) type;
                List<RelDataTypeField> pathFields = toRecordFields(pathType.getFields(), typeFactory);
                return new PathRecordType(pathFields);
            case Types.TYPE_NAME_GRAPH:
                GraphSchema graphSchema = (GraphSchema) type;
                List<RelDataTypeField> recordFields = toRecordFields(graphSchema.getFields(), typeFactory);
                return new GraphRecordType(graphSchema.getGraphName(), recordFields);
            default:
                if (type.isPrimitive()) {
                    String sqlTypeName = convertToSqlTypeName(type);
                    SqlTypeName typeName = Types.getType(type.getTypeClass()) == Types.BINARY_STRING
                        ? SqlTypeName.get(sqlTypeName, ((BinaryStringType) type).getPrecision())
                        : SqlTypeName.get(sqlTypeName);
                    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(typeName), isNullable);
                } else {
                    throw new GeaFlowDSLException("Not support type: " + type);
                }
        }
    }

    private static List<RelDataTypeField> toRecordFields(List<TableField> tableFields,
                                                         RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> recordFields = new ArrayList<>(tableFields.size());
        for (int i = 0; i < tableFields.size(); i++) {
            TableField tableField = tableFields.get(i);
            recordFields.add(new RelDataTypeFieldImpl(tableField.getName(), i,
                convertToRelType(tableField.getType(), tableField.isNullable(), typeFactory)));
        }
        return recordFields;
    }

    public static String convertTypeName(String sqlTypeName) {
        String upperName = sqlTypeName.toUpperCase(Locale.ROOT);
        if (upperName.equals(SqlTypeName.VARCHAR.getName())
            || upperName.equals(SqlTypeName.CHAR.getName())
            || upperName.equals(Types.TYPE_NAME_STRING)) {
            return Types.TYPE_NAME_BINARY_STRING;
        }
        if (upperName.equals(SqlTypeName.BIGINT.getName())) {
            return Types.TYPE_NAME_LONG;
        }
        if (upperName.equals(SqlTypeName.DECIMAL.getName())) {
            return Types.TYPE_NAME_DECIMAL;
        }
        if (upperName.startsWith("CHAR(") || upperName.startsWith("VARCHAR(")) {
            return Types.TYPE_NAME_BINARY_STRING;
        }
        if (upperName.equals("INT") || upperName.equals("SYMBOL")) {
            return Types.TYPE_NAME_INTEGER;
        }
        return upperName;
    }

    private static String convertToSqlTypeName(IType<?> type) {
        switch (type.getName()) {
            case Types.TYPE_NAME_STRING:
            case Types.TYPE_NAME_BINARY_STRING:
                return SqlTypeName.VARCHAR.getName();
            case Types.TYPE_NAME_LONG:
                return SqlTypeName.BIGINT.getName();
            default:
                return type.getName().toUpperCase();
        }
    }

    public static List<Class<?>> convertToJavaTypes(List<RelDataType> types,
                                                    JavaTypeFactory typeFactory) {
        List<Class<?>> javaTypes = new ArrayList<>();
        for (RelDataType type : types) {
            javaTypes.add((Class<?>) typeFactory.getJavaClass(type));
        }
        return javaTypes;
    }

    public static List<Class<?>> convertToJavaTypes(RelDataType rowType,
                                                    JavaTypeFactory typeFactory) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        List<RelDataType> types = new ArrayList<>();
        for (RelDataTypeField field : fields) {
            types.add(field.getType());
        }
        return convertToJavaTypes(types, typeFactory);
    }
}
