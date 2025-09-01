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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;

public class StructType implements IType<Row> {

    protected final List<TableField> fields;

    public StructType(List<TableField> fields) {
        this.fields = ImmutableList.copyOf(fields);
        Set<String> names = fields.stream().map(TableField::getName).collect(Collectors.toSet());
        Preconditions.checkArgument(names.size() == fields.size(),
            "Duplicate fields found for struct type.");
    }

    public StructType(TableField... fields) {
        this(ImmutableList.copyOf(fields));
    }

    public static StructType singleValue(IType<?> valueType, boolean nullable) {
        return new StructType(new TableField("col_0", valueType, nullable));
    }

    public int indexOf(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    public boolean contain(String name) {
        return indexOf(name) != -1;
    }

    public TableField getField(int i) {
        if (i >= 0 && i <= fields.size() - 1) {
            return fields.get(i);
        }
        throw new IndexOutOfBoundsException("Index: " + i + ", size: " + fields.size());
    }

    public TableField getField(String name) {
        int index = indexOf(name);
        if (index == -1) {
            throw new IllegalArgumentException("Field: '" + name + "' is not exist");
        }
        return fields.get(index);
    }

    public List<TableField> getFields() {
        return fields;
    }

    public IType<?> getType(int i) {
        return getField(i).getType();
    }

    public IType<?>[] getTypes() {
        IType<?>[] types = new IType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            types[i] = fields.get(i).getType();
        }
        return types;
    }

    public StructType addField(TableField field) {
        List<TableField> newFields = new ArrayList<>(fields);
        newFields.add(field);
        return new StructType(newFields);
    }

    public StructType replace(String name, TableField newField) {
        int index = indexOf(name);
        if (index == -1) {
            throw new IllegalArgumentException("Field: '" + name + "' is not exist");
        }
        List<TableField> newFields = new ArrayList<>(fields);
        newFields.set(index, newField);
        return new StructType(newFields);
    }

    public StructType dropRight(int size) {
        return new StructType(fields.subList(0, fields.size() - size));
    }

    public List<String> getFieldNames() {
        return fields.stream().map(TableField::getName).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (TableField field : fields) {
            if (!first) {
                sb.append(",");
            }
            sb.append(field.getName()).append(":{type:").append(field.getType()).append(",")
                .append("nullable:").append(field.isNullable()).append("}");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    public int size() {
        return fields.size();
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_STRUCT;
    }

    @Override
    public Class<Row> getTypeClass() {
        return Row.class;
    }

    @Override
    public byte[] serialize(Row obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public Row deserialize(byte[] bytes) {
        return (Row) SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @Override
    public int compare(Row a, Row b) {
        if (null == a) {
            return b == null ? 0 : -1;
        } else if (b == null) {
            return 1;
        } else {
            for (int i = 0; i < fields.size(); i++) {
                IType<?> type = fields.get(i).getType();
                int comparator = ((IType) type).compare(a.getField(i, type), b.getField(i, type));
                if (comparator != 0) {
                    return comparator;
                }
            }
            return 0;
        }
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    public StructType merge(StructType other) {
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
        return new StructType(mergedFields);
    }

    public List<TableField> getAddingFields(StructType baseType) {
        Map<String, TableField> baseFields = new HashMap<>();
        for (TableField field : baseType.fields) {
            baseFields.put(field.getName(), field);
        }
        List<TableField> addingFields = new ArrayList<>();
        for (TableField field : fields) {
            if (!baseFields.containsKey(field.getName())) {
                addingFields.add(field);
            }
        }
        return addingFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StructType)) {
            return false;
        }
        StructType that = (StructType) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
