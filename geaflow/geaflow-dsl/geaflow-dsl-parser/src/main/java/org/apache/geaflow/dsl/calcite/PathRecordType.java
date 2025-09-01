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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public class PathRecordType extends RelRecordType {

    public static final PathRecordType EMPTY = new PathRecordType(Collections.emptyList());

    public PathRecordType(List<RelDataTypeField> fields) {
        super(StructKind.PEEK_FIELDS, fields);
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.PATH;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb.append("Path:"), withDetail);
    }

    public PathRecordType copy(int index, RelDataType newType) {
        List<RelDataTypeField> fields = getFieldList();
        RelDataTypeField field = fields.get(index);

        if (field.getType().getSqlTypeName() != newType.getSqlTypeName()) {
            throw new IllegalArgumentException("Cannot replace field: " + field.getName()
                + " with different typename");
        }
        RelDataTypeField newField = new RelDataTypeFieldImpl(field.getName(), index, newType);

        List<RelDataTypeField> newFields = new ArrayList<>(fields);
        newFields.set(index, newField);
        return new PathRecordType(newFields);
    }

    public boolean canConcat(PathRecordType other) {
        return (this.lastFieldName().equals(other.firstFieldName()))
            && this.isSinglePath() && other.isSinglePath();
    }

    public PathRecordType concat(PathRecordType other, boolean caseSensitive) {
        List<RelDataTypeField> newFields = new ArrayList<>(getFieldList());
        Set<String> fieldNames = getFieldNames().stream().map(name -> {
            if (caseSensitive) {
                return name;
            } else {
                return name.toUpperCase(Locale.ROOT);
            }
        }).collect(Collectors.toSet());

        for (RelDataTypeField field : other.getFieldList()) {
            String name = caseSensitive ? field.getName() : field.getName().toUpperCase(Locale.ROOT);
            if (!fieldNames.contains(name)) {
                int index = newFields.size();
                newFields.add(field.copy(index));
            }
        }
        return new PathRecordType(newFields);
    }

    public PathRecordType join(PathRecordType other, RelDataTypeFactory typeFactory) {
        RelDataType joinType = SqlValidatorUtil.deriveJoinRowType(this, other,
            JoinRelType.INNER, typeFactory, null, Collections.emptyList());
        return new JoinPathRecordType(joinType.getFieldList());
    }

    public PathRecordType addField(String name, RelDataType type, boolean caseSensitive) {
        List<RelDataTypeField> newFields = new ArrayList<>(getFieldList());
        RelDataTypeField field = getField(name, caseSensitive, false);
        if (field != null) {
            newFields.set(field.getIndex(), new RelDataTypeFieldImpl(name, field.getIndex(), type));
        } else {
            newFields.add(new RelDataTypeFieldImpl(name, newFields.size(), type));
        }
        return new PathRecordType(newFields);
    }

    public Optional<String> firstFieldName() {
        if (fieldList.size() == 0) {
            return Optional.empty();
        }
        return Optional.of(fieldList.get(0).getName());
    }

    public Optional<String> lastFieldName() {
        if (fieldList.size() == 0) {
            return Optional.empty();
        }
        return Optional.of(fieldList.get(fieldList.size() - 1).getName());
    }

    public Optional<RelDataTypeField> firstField() {
        if (fieldList.size() == 0) {
            return Optional.empty();
        }
        return Optional.of(fieldList.get(0));
    }

    public Optional<RelDataTypeField> lastField() {
        if (fieldList.size() == 0) {
            return Optional.empty();
        }
        return Optional.of(fieldList.get(fieldList.size() - 1));
    }

    public boolean isSinglePath() {
        return true;
    }
}
