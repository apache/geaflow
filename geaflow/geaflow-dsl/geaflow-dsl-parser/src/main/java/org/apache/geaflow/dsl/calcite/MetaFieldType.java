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

import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

public class MetaFieldType extends RelDataTypeImpl {

    private final MetaField metaField;

    private final RelDataType type;

    private MetaFieldType(MetaField metaField, RelDataType type) {
        this.metaField = metaField;
        this.type = type;
        computeDigest();
    }

    public static MetaFieldType vertexId(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.VERTEX_ID, typeFactory.createTypeWithNullability(type, false));
    }

    public static MetaFieldType vertexType(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.VERTEX_TYPE, typeFactory.createTypeWithNullability(type, false));
    }

    public static MetaFieldType edgeSrcId(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.EDGE_SRC_ID, typeFactory.createTypeWithNullability(type, false));
    }

    public static MetaFieldType edgeTargetId(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.EDGE_TARGET_ID, typeFactory.createTypeWithNullability(type, false));
    }

    public static MetaFieldType edgeType(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.EDGE_TYPE, typeFactory.createTypeWithNullability(type, false));
    }

    public static MetaFieldType edgeTs(RelDataType type, RelDataTypeFactory typeFactory) {
        return new MetaFieldType(MetaField.EDGE_TS, typeFactory.createTypeWithNullability(type, false));
    }

    @Override
    public void computeDigest() {
        if (type instanceof RelDataTypeImpl) {
            ((RelDataTypeImpl) type).computeDigest();
            this.digest = ((RelDataTypeImpl) type).getDigest();
        }
    }

    @Override
    public boolean isStruct() {
        return type.isStruct();
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
        return type.getFieldList();
    }

    @Override
    public List<String> getFieldNames() {
        return type.getFieldNames();
    }

    @Override
    public int getFieldCount() {
        return type.getFieldCount();
    }

    @Override
    public StructKind getStructKind() {
        return type.getStructKind();
    }

    @Override
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        return type.getField(fieldName, caseSensitive, elideRecord);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public RelDataType getComponentType() {
        return type.getComponentType();
    }

    @Override
    public RelDataType getKeyType() {
        return type.getKeyType();
    }

    @Override
    public RelDataType getValueType() {
        return type.getValueType();
    }

    @Override
    public Charset getCharset() {
        return type.getCharset();
    }

    @Override
    public SqlCollation getCollation() {
        return type.getCollation();
    }

    @Override
    public SqlIntervalQualifier getIntervalQualifier() {
        return type.getIntervalQualifier();
    }

    @Override
    public int getPrecision() {
        return type.getPrecision();
    }

    @Override
    public int getScale() {
        return type.getScale();
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return type.getSqlTypeName();
    }

    @Override
    public SqlIdentifier getSqlIdentifier() {
        return type.getSqlIdentifier();
    }

    @Override
    public String toString() {
        return type.toString();
    }

    @Override
    public String getFullTypeString() {
        return type.toString();
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return type.getFamily();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        if (type instanceof RelDataTypeImpl) {
            ((RelDataTypeImpl) type).generateTypeString2(sb, withDetail);
        } else {
            sb.append(type.toString());
        }
    }

    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return type.getPrecedenceList();
    }

    @Override
    public RelDataTypeComparability getComparability() {
        return type.getComparability();
    }

    @Override
    public boolean isDynamicStruct() {
        return type.isDynamicStruct();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelDataType)) {
            return false;
        }
        RelDataType that = (RelDataType) o;
        return Objects.equals(type, that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    public MetaField getMetaField() {
        return metaField;
    }

    public RelDataType getType() {
        return type;
    }

    public enum MetaField {
        VERTEX_ID,
        VERTEX_TYPE,
        EDGE_SRC_ID,
        EDGE_TARGET_ID,
        EDGE_TYPE,
        EDGE_TS
    }
}
