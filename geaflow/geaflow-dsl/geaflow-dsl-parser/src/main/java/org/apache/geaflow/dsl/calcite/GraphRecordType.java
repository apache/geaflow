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

package org.apache.geaflow.dsl.calcite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.VertexType;

public class GraphRecordType extends RelRecordType {

    private static final Set<String> META_FIELD_NAMES = new HashSet<String>() {
        {
            add(VertexType.DEFAULT_ID_FIELD_NAME.toUpperCase(Locale.ROOT));
            add(EdgeType.DEFAULT_SRC_ID_NAME.toUpperCase(Locale.ROOT));
            add(EdgeType.DEFAULT_TARGET_ID_NAME.toUpperCase(Locale.ROOT));
            add(EdgeType.DEFAULT_LABEL_NAME.toUpperCase(Locale.ROOT));
            add(EdgeType.DEFAULT_TS_NAME.toUpperCase(Locale.ROOT));
        }
    };

    private final String graphName;

    public static void validateFieldName(String name) {
        if (META_FIELD_NAMES.contains(name.toUpperCase(Locale.ROOT))) {
            throw new GeaFlowDSLException("Field {} cannot use in graph as field name.", name);
        }
    }

    public GraphRecordType(String graphName, List<RelDataTypeField> fields) {
        super(StructKind.PEEK_FIELDS, fields);
        this.graphName = Objects.requireNonNull(graphName);
    }

    public RelDataTypeField getField(List<String> fields, boolean caseSensitive) {
        RelDataTypeField field = super.getField(fields.get(0), caseSensitive, false);
        if (field != null) {
            RelDataType fieldType = field.getType();
            for (int i = 1; i < fields.size(); i++) {
                field = fieldType.getField(fields.get(i), caseSensitive, false);
                if (field == null) {
                    return null;
                }
                fieldType = field.getType();
            }
        }
        return field;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("Graph:");
        super.generateTypeString(sb, withDetail);
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.GRAPH;
    }

    public String getGraphName() {
        return graphName;
    }

    public VertexRecordType getVertexType(Collection<String> vertexTypes, RelDataTypeFactory typeFactory) {
        for (String vertexType : vertexTypes) {
            boolean exist = false;
            for (RelDataTypeField field : getFieldList()) {
                if (field.getType() instanceof VertexRecordType) {
                    if (field.getName().equals(vertexType)) {
                        exist = true;
                        break;
                    }
                }
            }
            if (!exist) {
                throw new GeaFlowDSLException("Cannot find vertex type: '" + vertexType
                    + "'.");
            }
        }

        List<RelDataTypeField> vertexTables = new ArrayList<>();
        for (RelDataTypeField field : getFieldList()) {
            if (field.getType() instanceof VertexRecordType) {
                if (vertexTypes.isEmpty() || vertexTypes.contains(field.getName())) {
                    vertexTables.add(field);
                }
            }
        }

        //Check all vertex tables to be merged have the same ID field name and type
        String chkVertexId = null;
        RelDataType chkVertexIdType = null;
        for (RelDataTypeField field : vertexTables) {
            VertexRecordType vertexType = (VertexRecordType) field.getType();
            if (chkVertexId == null) {
                chkVertexId = vertexType.getIdField().getName();
                chkVertexIdType = vertexType.getIdField().getType();
            } else {
                if (!vertexType.getIdField().getType().equals(chkVertexIdType)) {
                    throw new GeaFlowDSLException("Id field type should be same between vertex "
                        + "tables");
                }
            }
        }

        Map<String, RelDataType> existFields = new HashMap<>();
        List<RelDataTypeField> combineFields = new ArrayList<>();
        String idField = null;
        for (RelDataTypeField field : vertexTables) {
            VertexRecordType vertexType = (VertexRecordType) field.getType();
            idField = vertexType.getIdField().getName();

            List<RelDataTypeField> vertexFields = vertexType.getFieldList();
            for (RelDataTypeField vertexField : vertexFields) {
                if (existFields.containsKey(vertexField.getName())) {
                    // The field with the same name and type between vertex tables
                    // will be merged as one field. It's illegal for different types
                    // of same name fields between vertex tables.
                    if (!existFields.get(vertexField.getName()).equals(vertexField.getType())) {
                        throw new GeaFlowDSLException("Same name field between vertex tables "
                            + "shouldn't have different type.");
                    }
                } else {
                    existFields.put(vertexField.getName(), vertexField.getType());
                    combineFields.add(vertexField);
                }
            }
        }

        return VertexRecordType.createVertexType(combineFields, idField, typeFactory);
    }

    public EdgeRecordType getEdgeType(Collection<String> edgeTypes, RelDataTypeFactory typeFactory) {
        for (String edgeType : edgeTypes) {
            boolean exist = false;
            for (RelDataTypeField field : getFieldList()) {
                if (field.getType() instanceof EdgeRecordType) {
                    if (field.getName().equals(edgeType)) {
                        exist = true;
                        break;
                    }
                }
            }
            if (!exist) {
                throw new GeaFlowDSLException("Cannot find edge type: '" + edgeType
                    + "'.");
            }
        }
        List<RelDataTypeField> edgeTables = new ArrayList<>();
        for (RelDataTypeField field : getFieldList()) {
            if (field.getType() instanceof EdgeRecordType) {
                if (edgeTypes.isEmpty() || edgeTypes.contains(field.getName())) {
                    edgeTables.add(field);
                }
            }
        }
        // Check all edge tables to be merged have the same SOURCE ID / DESTINATION ID
        // / TIMESTAMP field name and type
        String chkSourceId = null;
        String chkDestinationId = null;
        String chkTimestamp = null;
        Boolean definedTimestamp = null;
        RelDataType chkSourceIdType = null;
        RelDataType chkDestinationIdType = null;
        RelDataType chkTimestampType = null;
        for (RelDataTypeField field : edgeTables) {
            EdgeRecordType edgeType = (EdgeRecordType) field.getType();
            if (chkSourceId == null) {
                chkSourceId = edgeType.getSrcIdField().getName();
                chkSourceIdType = edgeType.getSrcIdField().getType();
            } else {
                if (!edgeType.getSrcIdField().getType().equals(chkSourceIdType)) {
                    throw new GeaFlowDSLException("SOURCE ID field type should be same between edge "
                        + "tables");
                }
            }
            if (chkDestinationId == null) {
                chkDestinationId = edgeType.getTargetIdField().getName();
                chkDestinationIdType = edgeType.getTargetIdField().getType();
            } else {
                if (!edgeType.getTargetIdField().getType().equals(chkDestinationIdType)) {
                    throw new GeaFlowDSLException("DESTINATION ID field type should be same "
                        + "between edge tables");
                }
            }
            if (definedTimestamp == null) {
                definedTimestamp = edgeType.getTimestampField().isPresent();
            } else if (definedTimestamp != edgeType.getTimestampField().isPresent()) {
                throw new GeaFlowDSLException("TIMESTAMP should defined or not defined in all edge tables");
            }
            if (definedTimestamp) {
                if (chkTimestamp == null) {
                    chkTimestamp = edgeType.getTimestampField().get().getName();
                    chkTimestampType = edgeType.getTimestampField().get().getType();
                } else {
                    if (!edgeType.getTimestampField().get().getType().equals(chkTimestampType)) {
                        throw new GeaFlowDSLException("TIMESTAMP field type should be same between edge "
                            + "tables");
                    }
                }
            }
        }

        Map<String, RelDataType> existFields = new HashMap<>();
        List<RelDataTypeField> combineFields = new ArrayList<>();
        String srcIdField = null;
        String targetField = null;
        String tsField = null;

        for (RelDataTypeField field : edgeTables) {
            EdgeRecordType edgeType = (EdgeRecordType) field.getType();
            srcIdField = edgeType.getSrcIdField().getName();
            targetField = edgeType.getTargetIdField().getName();
            tsField = edgeType.getTimestampField().map(RelDataTypeField::getName).orElse(null);

            List<RelDataTypeField> edgeFields = edgeType.getFieldList();
            for (RelDataTypeField edgeField : edgeFields) {
                if (existFields.containsKey(edgeField.getName())) {
                    // The field with the same name and type between edge tables
                    // will be merged as one field. It's illegal for different types
                    // of same name fields between edge tables.
                    if (!existFields.get(edgeField.getName()).equals(edgeField.getType())) {
                        throw new GeaFlowDSLException("Same name field between edge tables "
                            + "shouldn't have different type.");
                    }
                } else {
                    existFields.put(edgeField.getName(), edgeField.getType());
                    combineFields.add(edgeField);
                }
            }
        }

        return EdgeRecordType.createEdgeType(combineFields, srcIdField, targetField, tsField, typeFactory);
    }

    public static List<RelDataTypeField> renameMetaField(List<RelDataTypeField> fields,
                                                         MetaField metaType,
                                                         String newFieldName) {
        List<RelDataTypeField> metaFields = new ArrayList<>();
        return fields.stream().filter(f -> {
            if (f.getType() instanceof MetaFieldType
                && ((MetaFieldType) f.getType()).getMetaField().equals(metaType)) {
                if (metaFields.isEmpty()) {
                    metaFields.add(f);
                    return true;
                }
                return false;
            }
            return true;
        }).map(f -> {
            if (f.getType() instanceof MetaFieldType
                && ((MetaFieldType) f.getType()).getMetaField().equals(metaType)) {
                return new RelDataTypeFieldImpl(newFieldName, f.getIndex(), f.getType());
            } else {
                return f;
            }
        }).collect(Collectors.toList());
    }

    /**
     * Copy the graph type and add a vertex field to all the vertex tables.
     *
     * @param fieldName The added field name.
     * @param fieldType The added field type.
     */
    public GraphRecordType addVertexField(String fieldName, RelDataType fieldType) {
        List<RelDataTypeField> fields = getFieldList();
        List<RelDataTypeField> newFields = new ArrayList<>();
        for (RelDataTypeField field : fields) {
            if (field.getType().getSqlTypeName() == SqlTypeName.VERTEX) {
                VertexRecordType vertexRecordType = (VertexRecordType) field.getType();
                VertexRecordType newVertexType = vertexRecordType.add(fieldName,
                    fieldType, false);
                newFields.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newVertexType));
            } else {
                newFields.add(field);
            }
        }
        return new GraphRecordType(graphName, newFields);
    }
}
