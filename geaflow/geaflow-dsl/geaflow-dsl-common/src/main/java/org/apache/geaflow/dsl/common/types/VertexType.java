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

package org.apache.geaflow.dsl.common.types;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class VertexType extends StructType {

    public static final int ID_FIELD_POSITION = 0;

    public static final int LABEL_FIELD_POSITION = 1;

    private static final int NUM_META_FIELDS = 2;

    public static final String DEFAULT_ID_FIELD_NAME = "~id";

    public VertexType(List<TableField> fields) {
        super(fields);
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_VERTEX;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Row> getTypeClass() {
        return (Class) RowVertex.class;
    }

    public TableField getId() {
        return getField(ID_FIELD_POSITION);
    }

    public TableField getLabel() {
        return getField(LABEL_FIELD_POSITION);
    }

    public int getValueSize() {
        return size() - getValueOffset();
    }

    public int getValueOffset() {
        return NUM_META_FIELDS;
    }

    public IType<?>[] getValueTypes() {
        IType<?>[] valueTypes = new IType[size() - getValueOffset()];
        for (int i = getValueOffset(); i < size(); i++) {
            valueTypes[i - getValueOffset()] = fields.get(i).getType();
        }
        return valueTypes;
    }

    public List<TableField> getValueFields() {
        return getFields().subList(getValueOffset(), size());
    }

    public static VertexType emptyVertex(IType<?> idType) {
        TableField idField = new TableField("~id", idType, false);
        TableField labelField = new TableField("~label", Types.STRING, false);
        return new VertexType(Lists.newArrayList(idField, labelField));
    }

    @Override
    public VertexType merge(StructType other) {
        assert other instanceof VertexType : "VertexType should merge with vertex type";
        Map<String, IType<?>> name2Types = new HashMap<>();
        for (TableField field : this.fields) {
            name2Types.put(field.getName(), field.getType());
        }
        List<TableField> mergedFields = new ArrayList<>(this.fields);
        for (TableField field : other.fields) {
            if (name2Types.containsKey(field.getName())) {
                if (!name2Types.get(field.getName()).equals(field.getType())) {
                    throw new IllegalArgumentException("Fail to merge vertex schema");
                }
            } else {
                mergedFields.add(field);
            }
        }
        return new VertexType(mergedFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VertexType)) {
            return false;
        }
        VertexType that = (VertexType) o;
        return Objects.equals(fields, that.fields);
    }
}
