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
import java.util.Optional;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;

public class EdgeType extends StructType {

    public static final int SRC_ID_FIELD_POSITION = 0;

    public static final int TARGET_ID_FIELD_POSITION = 1;

    public static final int LABEL_FIELD_POSITION = 2;

    public static final int TIME_FIELD_POSITION = 3;

    private final boolean hasTimestamp;

    private static final int NUM_META_FIELDS_WITHOUT_TS = 3;

    private static final int NUM_META_FIELDS_WITH_TS = 4;

    public static final String DEFAULT_SRC_ID_NAME = "~srcId";

    public static final String DEFAULT_TARGET_ID_NAME = "~targetId";

    public static final String DEFAULT_LABEL_NAME = "~label";

    public static final String DEFAULT_TS_NAME = "~ts";

    public EdgeType(List<TableField> fields, boolean hasTimestamp) {
        super(fields);
        this.hasTimestamp = hasTimestamp;
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_EDGE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Row> getTypeClass() {
        return (Class) RowEdge.class;
    }

    public TableField getSrcId() {
        return fields.get(SRC_ID_FIELD_POSITION);
    }

    public TableField getTargetId() {
        return fields.get(TARGET_ID_FIELD_POSITION);
    }

    public TableField getLabel() {
        return fields.get(LABEL_FIELD_POSITION);
    }

    public Optional<TableField> getTimestamp() {
        if (hasTimestamp) {
            return Optional.of(fields.get(TIME_FIELD_POSITION));
        }
        return Optional.empty();
    }

    public int getValueSize() {
        return size() - getValueOffset();
    }

    public int getValueOffset() {
        if (hasTimestamp) {
            return NUM_META_FIELDS_WITH_TS;
        }
        return NUM_META_FIELDS_WITHOUT_TS;
    }

    public List<TableField> getValueFields() {
        return getFields().subList(getValueOffset(), size());
    }

    public IType<?>[] getValueTypes() {
        IType<?>[] valueTypes = new IType[size() - getValueOffset()];
        List<TableField> valueFields = getValueFields();
        for (int i = 0; i < valueFields.size(); i++) {
            valueTypes[i] = valueFields.get(i).getType();
        }
        return valueTypes;
    }

    public static EdgeType emptyEdge(IType<?> idType) {
        TableField srcField = new TableField(DEFAULT_SRC_ID_NAME, idType, false);
        TableField targetField = new TableField(DEFAULT_TARGET_ID_NAME, idType, false);
        TableField labelField = new TableField(GraphSchema.LABEL_FIELD_NAME, Types.STRING, false);
        return new EdgeType(Lists.newArrayList(srcField, targetField, labelField), false);
    }

    @Override
    public EdgeType merge(StructType other) {
        assert other instanceof EdgeType : "EdgeType should merge with edge type";
        assert hasTimestamp && ((EdgeType) other).hasTimestamp
            || !hasTimestamp && !((EdgeType) other).hasTimestamp : "Cannot merge different edge type";
        Map<String, IType<?>> name2Types = new HashMap<>();
        for (TableField field : this.fields) {
            name2Types.put(field.getName(), field.getType());
        }
        List<TableField> mergedFields = new ArrayList<>(this.fields);
        for (TableField field : other.fields) {
            if (name2Types.containsKey(field.getName())) {
                if (!name2Types.get(field.getName()).equals(field.getType())) {
                    throw new IllegalArgumentException("Fail to merge edge schema");
                }
            } else {
                mergedFields.add(field);
            }
        }
        return new EdgeType(mergedFields, hasTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EdgeType)) {
            return false;
        }
        EdgeType that = (EdgeType) o;
        return Objects.equals(fields, that.fields);
    }

}
