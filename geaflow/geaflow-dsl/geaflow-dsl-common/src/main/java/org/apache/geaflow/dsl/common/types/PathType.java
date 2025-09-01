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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;

public class PathType extends StructType {

    public static final PathType EMPTY = new PathType();

    public PathType(List<TableField> fields) {
        super(fields);
    }

    public PathType(TableField... fields) {
        super(fields);
    }

    @Override
    public PathType addField(TableField field) {
        List<TableField> newFields = new ArrayList<>(fields);
        newFields.add(field);
        return new PathType(newFields);
    }

    @Override
    public PathType replace(String name, TableField newField) {
        int index = indexOf(name);
        if (index == -1) {
            throw new IllegalArgumentException("Field: '" + name + "' is not exist on the path");
        }
        List<TableField> newFields = new ArrayList<>(fields);
        newFields.set(index, newField);
        return new PathType(newFields);
    }

    public PathType filter(Predicate<TableField> predicate) {
        List<TableField> filterFields = fields.stream()
            .filter(predicate)
            .collect(Collectors.toList());
        return new PathType(filterFields);
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_PATH;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Row> getTypeClass() {
        return (Class) Path.class;
    }

    public Set<String> getCommonFieldNames(PathType other) {
        Set<String> fieldNames = new HashSet<>(getFieldNames());
        Set<String> otherFieldNames = new HashSet<>(other.getFieldNames());
        return Sets.intersection(fieldNames, otherFieldNames);
    }

    public PathType join(PathType right) {
        List<TableField> joinFields = new ArrayList<>(fields);

        Map<String, Integer> nameCount = new HashMap<>();
        for (TableField rightField : right.fields) {
            if (joinFields.contains(rightField)) {
                int cnt = nameCount.getOrDefault(rightField.getName(), 0);
                String newName = rightField.getName() + cnt;
                joinFields.add(rightField.copy(newName));
                nameCount.put(rightField.getName(), cnt + 1);
            } else {
                joinFields.add(rightField);
            }
        }
        return new PathType(joinFields);
    }

    public PathType subPath(int from, int size) {
        List<TableField> subFields = fields.subList(from, from + size);
        return new PathType(subFields);
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
    public PathType merge(StructType other) {
        assert other instanceof PathType : "PathType should merge with path type";
        Map<String, IType<?>> name2Types = new HashMap<>();
        for (TableField field : this.fields) {
            name2Types.put(field.getName(), field.getType());
        }
        List<TableField> mergedFields = new ArrayList<>(this.fields);
        for (TableField field : other.fields) {
            if (name2Types.containsKey(field.getName())) {
                if (!name2Types.get(field.getName()).equals(field.getType())) {
                    throw new IllegalArgumentException("Fail to merge path schema");
                }
            } else {
                mergedFields.add(field);
            }
        }
        return new PathType(mergedFields);
    }
}
