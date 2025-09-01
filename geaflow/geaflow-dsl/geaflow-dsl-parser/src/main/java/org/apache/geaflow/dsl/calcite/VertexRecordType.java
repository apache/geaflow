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

import static org.apache.geaflow.dsl.calcite.MetaFieldType.vertexId;
import static org.apache.geaflow.dsl.calcite.MetaFieldType.vertexType;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.VertexType;

public class VertexRecordType extends RelRecordType {

    private VertexRecordType(List<RelDataTypeField> fields) {
        super(StructKind.PEEK_FIELDS, fields);
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    public static VertexRecordType createVertexType(List<RelDataTypeField> fields, String idField,
                                                    RelDataTypeFactory typeFactory) {
        boolean hasMultiIdFields = fields.stream().filter(f -> f.getType() instanceof MetaFieldType
            && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.VERTEX_ID)).count() > 1;
        if (hasMultiIdFields) {
            idField = VertexType.DEFAULT_ID_FIELD_NAME;
            fields = GraphRecordType.renameMetaField(fields, MetaField.VERTEX_ID, idField);
        }
        List<RelDataTypeField> reorderFields = reorderFields(fields, idField, typeFactory);
        return new VertexRecordType(reorderFields);
    }

    public static VertexRecordType createVertexType(List<RelDataTypeField> fields, RelDataTypeFactory typeFactory) {
        String idField = null;
        for (RelDataTypeField field : fields) {
            if (field.getType() instanceof MetaFieldType
                && ((MetaFieldType) field.getType()).getMetaField() == MetaField.VERTEX_ID) {
                idField = field.getName();
            }
        }
        Preconditions.checkArgument(idField != null, "Missing id field");
        return createVertexType(fields, idField, typeFactory);
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.VERTEX;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb.append("Vertex:"), withDetail);
    }

    public RelDataTypeField getIdField() {
        return fieldList.get(VertexType.ID_FIELD_POSITION);
    }

    public RelDataTypeField getLabelField() {
        return fieldList.get(VertexType.LABEL_FIELD_POSITION);
    }

    public boolean isId(int index) {
        return index == VertexType.ID_FIELD_POSITION;
    }

    public VertexRecordType add(String fieldName, RelDataType type, boolean caseSensitive) {
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
        return new VertexRecordType(fields);
    }

    private static List<RelDataTypeField> reorderFields(List<RelDataTypeField> fields, String idField,
                                                        RelDataTypeFactory typeFactory) {
        if (fields == null) {
            throw new NullPointerException("fields is null");
        }
        List<RelDataTypeField> reorderFields = new ArrayList<>(fields.size());
        int idIndex = EdgeRecordType.indexOf(fields, idField);
        assert idIndex != -1 : "idField: " + idField + " is not exist";
        RelDataTypeField idTypeField = fields.get(idIndex);

        // put id field at position 0.
        reorderFields.add(new RelDataTypeFieldImpl(idTypeField.getName(), VertexType.ID_FIELD_POSITION,
            vertexId(idTypeField.getType(), typeFactory)));
        // put label field at position 1.
        reorderFields.add(new RelDataTypeFieldImpl(GraphSchema.LABEL_FIELD_NAME, VertexType.LABEL_FIELD_POSITION,
            vertexType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory)));
        // put other fields by order exclude ~label.
        int labelIndex = EdgeRecordType.indexOf(fields, GraphSchema.LABEL_FIELD_NAME);
        for (int k = 0; k < fields.size(); k++) {
            RelDataTypeField field = fields.get(k);
            if (k != labelIndex && k != idIndex) {
                reorderFields.add(new RelDataTypeFieldImpl(field.getName(), reorderFields.size(), field.getType()));
            }
        }
        return reorderFields;
    }

    public static class VirtualVertexRecordType extends VertexRecordType {

        public static final String VIRTUAL_ID_FIELD_NAME = "~virtual_id";

        private VirtualVertexRecordType(List<RelDataTypeField> fields) {
            super(fields);
        }

        public static VirtualVertexRecordType of(RelDataTypeFactory typeFactory) {
            List<RelDataTypeField> fields = new ArrayList<>();
            fields.add(new RelDataTypeFieldImpl(VIRTUAL_ID_FIELD_NAME,
                VertexType.ID_FIELD_POSITION, typeFactory.createSqlType(SqlTypeName.ANY)));
            fields.add(new RelDataTypeFieldImpl("~label", VertexType.LABEL_FIELD_POSITION,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return new VirtualVertexRecordType(fields);
        }
    }
}
