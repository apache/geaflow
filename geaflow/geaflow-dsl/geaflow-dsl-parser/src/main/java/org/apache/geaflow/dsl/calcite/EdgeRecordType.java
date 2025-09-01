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

import static org.apache.geaflow.dsl.calcite.MetaFieldType.edgeSrcId;
import static org.apache.geaflow.dsl.calcite.MetaFieldType.edgeTargetId;
import static org.apache.geaflow.dsl.calcite.MetaFieldType.edgeTs;
import static org.apache.geaflow.dsl.calcite.MetaFieldType.edgeType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.apache.geaflow.dsl.common.types.GraphSchema;

public class EdgeRecordType extends RelRecordType {

    private final boolean hasTimeField;

    private EdgeRecordType(List<RelDataTypeField> fields, boolean hasTimeField) {
        super(StructKind.PEEK_FIELDS, fields);
        this.hasTimeField = hasTimeField;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    public static EdgeRecordType createEdgeType(List<RelDataTypeField> fields, String srcIdField,
                                                String targetIdField, String timestampField,
                                                RelDataTypeFactory typeFactory) {

        boolean hasMultiSrcIdFields = fields.stream()
            .filter(f -> f.getType() instanceof MetaFieldType
                && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
            .count() > 1;
        if (hasMultiSrcIdFields) {
            srcIdField = EdgeType.DEFAULT_SRC_ID_NAME;
            fields = GraphRecordType.renameMetaField(fields, MetaField.EDGE_SRC_ID, srcIdField);
        }
        boolean hasMultiTargetIdFields = fields.stream()
            .filter(f -> f.getType() instanceof MetaFieldType
                && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_TARGET_ID))
            .count() > 1;
        if (hasMultiTargetIdFields) {
            targetIdField = EdgeType.DEFAULT_TARGET_ID_NAME;
            fields = GraphRecordType.renameMetaField(fields, MetaField.EDGE_TARGET_ID, targetIdField);
        }
        boolean hasMultiTsFields = fields.stream()
            .filter(f -> f.getType() instanceof MetaFieldType
                && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_TS))
            .count() > 1;
        if (hasMultiTsFields) {
            timestampField = EdgeType.DEFAULT_TS_NAME;
            fields = GraphRecordType.renameMetaField(fields, MetaField.EDGE_TS, timestampField);
        }
        List<RelDataTypeField> reorderFields = reorderFields(fields, srcIdField, targetIdField, timestampField,
            typeFactory);
        return new EdgeRecordType(reorderFields, timestampField != null);
    }

    public static EdgeRecordType createEdgeType(List<RelDataTypeField> fields, RelDataTypeFactory typeFactory) {
        String srcIdField = null;
        String targetIdField = null;
        String timestampField = null;
        for (RelDataTypeField field : fields) {
            if (field.getType() instanceof MetaFieldType) {
                MetaFieldType metaFieldType = (MetaFieldType) field.getType();
                if (metaFieldType.getMetaField() == MetaField.EDGE_SRC_ID) {
                    srcIdField = field.getName();
                } else if (metaFieldType.getMetaField() == MetaField.EDGE_TARGET_ID) {
                    targetIdField = field.getName();
                } else if (metaFieldType.getMetaField() == MetaField.EDGE_TS) {
                    timestampField = field.getName();
                }
            }
        }
        if (srcIdField == null) {
            throw new GeaFlowDSLException("Missing source id field");
        }
        if (targetIdField == null) {
            throw new GeaFlowDSLException("Missing target id field");
        }
        return createEdgeType(fields, srcIdField, targetIdField, timestampField, typeFactory);
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.EDGE;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb.append("Edge: "), withDetail);
    }

    public RelDataTypeField getSrcIdField() {
        return fieldList.get(EdgeType.SRC_ID_FIELD_POSITION);
    }

    public RelDataTypeField getTargetIdField() {
        return fieldList.get(EdgeType.TARGET_ID_FIELD_POSITION);
    }

    public RelDataTypeField getLabelField() {
        return fieldList.get(EdgeType.LABEL_FIELD_POSITION);
    }

    public Optional<RelDataTypeField> getTimestampField() {
        if (hasTimeField) {
            return Optional.of(fieldList.get(EdgeType.TIME_FIELD_POSITION));
        }
        return Optional.empty();
    }

    public int getTimestampIndex() {
        if (hasTimeField) {
            return EdgeType.TIME_FIELD_POSITION;
        }
        return -1;
    }

    public EdgeRecordType add(String fieldName, RelDataType type, boolean caseSensitive) {
        if (type instanceof MetaFieldType) {
            type = ((MetaFieldType) type).getType();
        }
        List<RelDataTypeField> fields = new ArrayList<>(getFieldList());

        RelDataTypeField field = getField(fieldName, caseSensitive, false);
        if (field != null) {
            fields.set(field.getIndex(), new RelDataTypeFieldImpl(fieldName, field.getIndex(), type));
        } else {
            fields.add(new RelDataTypeFieldImpl(fieldName, fields.size(), type));
        }
        return new EdgeRecordType(fields, hasTimeField);
    }

    private static List<RelDataTypeField> reorderFields(List<RelDataTypeField> fields, String srcIdField,
                                                        String targetIdField, String timestampField,
                                                        RelDataTypeFactory typeFactory) {
        if (fields == null) {
            throw new NullPointerException("fields is null");
        }
        List<RelDataTypeField> reorderFields = new ArrayList<>(fields.size());

        int srcIdIndex = indexOf(fields, srcIdField);
        int targetIdIndex = indexOf(fields, targetIdField);
        assert srcIdIndex != -1 : "srcIdField:" + srcIdField + " is not found";
        assert targetIdIndex != -1 : "targetIdField:" + targetIdField + " is not found";

        RelDataTypeField srcIdTypeField = fields.get(srcIdIndex);
        RelDataTypeField targetIdTypeField = fields.get(targetIdIndex);
        // put srcId field.
        reorderFields.add(new RelDataTypeFieldImpl(srcIdTypeField.getName(), EdgeType.SRC_ID_FIELD_POSITION,
            edgeSrcId(srcIdTypeField.getType(), typeFactory)));
        // put targetId field.
        reorderFields.add(new RelDataTypeFieldImpl(targetIdTypeField.getName(), EdgeType.TARGET_ID_FIELD_POSITION,
            edgeTargetId(targetIdTypeField.getType(), typeFactory)));
        // put label field.
        reorderFields.add(new RelDataTypeFieldImpl(GraphSchema.LABEL_FIELD_NAME, EdgeType.LABEL_FIELD_POSITION,
            edgeType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory)));
        // put ts field if it has defined.
        int tsIndex = indexOf(fields, timestampField);
        if (tsIndex != -1) {
            RelDataTypeField tsTypeField = fields.get(tsIndex);
            reorderFields.add(new RelDataTypeFieldImpl(tsTypeField.getName(), EdgeType.TIME_FIELD_POSITION,
                edgeTs(tsTypeField.getType(), typeFactory)));
        }
        int labelIndex = indexOf(fields, GraphSchema.LABEL_FIELD_NAME);
        // put other fields by order exclude ~label.
        for (int k = 0; k < fields.size(); k++) {
            RelDataTypeField field = fields.get(k);
            if (k != srcIdIndex && k != targetIdIndex && k != labelIndex && k != tsIndex) {
                reorderFields.add(new RelDataTypeFieldImpl(field.getName(), reorderFields.size(), field.getType()));
            }
        }
        return reorderFields;
    }

    static int indexOf(List<RelDataTypeField> fields, String name) {
        if (name == null) {
            return -1;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    public static EdgeRecordType emptyEdgeType(RelDataType idType, RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(new RelDataTypeFieldImpl(EdgeType.DEFAULT_SRC_ID_NAME,
            EdgeType.SRC_ID_FIELD_POSITION, MetaFieldType.edgeSrcId(idType, typeFactory)));
        fields.add(new RelDataTypeFieldImpl(EdgeType.DEFAULT_TARGET_ID_NAME,
            EdgeType.TARGET_ID_FIELD_POSITION, MetaFieldType.edgeTargetId(idType, typeFactory)));
        fields.add(new RelDataTypeFieldImpl(GraphSchema.LABEL_FIELD_NAME, EdgeType.LABEL_FIELD_POSITION,
            typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return EdgeRecordType.createEdgeType(fields, EdgeType.DEFAULT_SRC_ID_NAME,
            EdgeType.DEFAULT_TARGET_ID_NAME, null, typeFactory);
    }
}
