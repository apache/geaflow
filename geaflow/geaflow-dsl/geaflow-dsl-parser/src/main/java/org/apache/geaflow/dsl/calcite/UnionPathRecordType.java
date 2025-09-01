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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public class UnionPathRecordType extends PathRecordType {

    private final List<PathRecordType> inputPathRecordTypes;

    public UnionPathRecordType(List<PathRecordType> unionPaths, RelDataTypeFactory typeFactory) {
        super(createUnionTypeFields(unionPaths, typeFactory));
        inputPathRecordTypes = Objects.requireNonNull(unionPaths);
    }

    public UnionPathRecordType(List<RelDataTypeField> fields, List<PathRecordType> unionPaths) {
        super(fields);
        inputPathRecordTypes = Objects.requireNonNull(unionPaths);
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    private static List<RelDataTypeField> createUnionTypeFields(Iterable<PathRecordType> unionPaths,
                                                                RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> unionFields = new ArrayList<>();
        Map<String, Integer> presented = new HashMap<>();
        for (PathRecordType pathType : unionPaths) {
            for (int i = 0; i < pathType.getFieldCount(); i++) {
                String fieldName = pathType.getFieldList().get(i).getName();
                if (!presented.containsKey(fieldName)) {
                    RelDataTypeField newUnionField =
                        pathType.getFieldList().get(i).copy(unionFields.size());
                    unionFields.add(newUnionField);
                    presented.put(fieldName, unionFields.size() - 1);
                } else {
                    //derive union type
                    RelDataTypeField sameNameField = unionFields.get(presented.get(fieldName));
                    List<RelDataTypeField> unionFieldsList = sameNameField.getType().getFieldList();
                    List<RelDataTypeField> newUnionFieldsList = new ArrayList<>(unionFieldsList);
                    List<RelDataTypeField> fieldsList =
                        pathType.getFieldList().get(i).getType().getFieldList();
                    for (RelDataTypeField field : fieldsList) {
                        boolean found = false;
                        for (RelDataTypeField unionField : unionFieldsList) {
                            if (field.getName().equals(unionField.getName())) {
                                if (!field.getType().equals(unionField.getType())) {
                                    throw new GeaFlowDSLException(
                                        "Encountered ambiguous field with the same name "
                                            + "but different type when generating the Union type. \n"
                                            + "Name: " + field.getName() + "\n"
                                            + "Type: " + field.getType());
                                }

                                found = true;
                            }
                        }
                        if (!found) {
                            newUnionFieldsList.add(field);
                        }
                    }
                    if (sameNameField.getType().getSqlTypeName() == SqlTypeName.VERTEX) {
                        unionFields.set(presented.get(fieldName),
                            new RelDataTypeFieldImpl(fieldName, sameNameField.getIndex(),
                                VertexRecordType.createVertexType(newUnionFieldsList, typeFactory)));
                    } else if (sameNameField.getType().getSqlTypeName() == SqlTypeName.EDGE) {
                        unionFields.set(presented.get(fieldName),
                            new RelDataTypeFieldImpl(fieldName, sameNameField.getIndex(),
                                EdgeRecordType.createEdgeType(newUnionFieldsList, typeFactory)));
                    } else {
                        throw new IllegalArgumentException("Illegal type: " + sameNameField.getType());
                    }
                }
            }
        }
        return unionFields;
    }

    @Override
    public boolean isSinglePath() {
        return false;
    }

    @Override
    public UnionPathRecordType addField(String name, RelDataType type, boolean caseSensitive) {
        throw new GeaFlowDSLException("Illegal call.");
    }

    public List<PathRecordType> getInputPathRecordTypes() {
        return inputPathRecordTypes;
    }
}
