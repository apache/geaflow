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

package org.apache.geaflow.dsl.rex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.operator.SqlEdgeConstructOperator;
import org.apache.geaflow.dsl.operator.SqlVertexConstructOperator;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class RexObjectConstruct extends RexCall {

    private static final RexLiteral DEFAULT_LABEL_VALUE = GQLRexUtil.createString("");

    private final List<VariableInfo> variableInfos;

    public RexObjectConstruct(RelDataType type,
                              List<? extends RexNode> operands,
                              Map<RexNode, VariableInfo> rex2VariableInfo) {
        super(type, createConstructOperator(type), reOrderOperands(type, operands));
        this.variableInfos = new ArrayList<>();
        for (RexNode operand : operands) {
            VariableInfo variableInfo = Objects.requireNonNull(rex2VariableInfo.get(operand));
            variableInfos.add(variableInfo);
        }
    }

    public RexObjectConstruct(RelDataType type,
                              List<? extends RexNode> operands,
                              List<VariableInfo> variableInfos) {
        super(type, createConstructOperator(type), operands);
        this.variableInfos = Objects.requireNonNull(variableInfos);
    }

    @Override
    public RexObjectConstruct clone(RelDataType type, List<RexNode> operands) {
        return new RexObjectConstruct(type, operands, variableInfos);
    }

    public List<VariableInfo> getVariableInfo() {
        return variableInfos;
    }

    public RexObjectConstruct merge(RexObjectConstruct input, int pathIndex, RexBuilder builder) {
        SqlTypeName typeName = getType().getSqlTypeName();
        if (typeName != input.getType().getSqlTypeName()) {
            throw new IllegalArgumentException("Fail to merge vertex with edge");
        }
        List<RelDataTypeField> fields = getType().getFieldList();
        List<RelDataTypeField> inputFields = input.getType().getFieldList();
        List<VariableInfo> inputVariables = input.variableInfos;

        Map<String, RexNode> name2InputRex = new LinkedHashMap<>();
        Map<String, VariableInfo> name2InputVar = new LinkedHashMap<>();
        Map<String, RelDataTypeField> name2InputField = new LinkedHashMap<>();

        for (int i = 0; i < inputFields.size(); i++) {
            name2InputRex.put(inputFields.get(i).getName(), input.getOperands().get(i));
            name2InputVar.put(inputVariables.get(i).getName(), inputVariables.get(i));
            name2InputField.put(inputFields.get(i).getName(), inputFields.get(i));
        }

        List<RexNode> mergedNodes = new ArrayList<>();
        List<VariableInfo> mergedVariableInfo = new ArrayList<>();
        List<RelDataTypeField> mergedFields = new ArrayList<>();

        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).getName();
            RexNode currentRex = operands.get(i);
            RexNode inputRex = name2InputRex.get(name);

            VariableInfo currentVar = variableInfos.get(i);
            VariableInfo inputVar = name2InputVar.get(name);

            RelDataTypeField currentField = fields.get(i);

            if (inputRex != null) {
                assert currentVar.equals(inputVar) : "Fail to merge variable: " + currentVar;

                RexNode mergedRex = mergeRexNode(inputRex, currentRex, pathIndex, name);
                mergedNodes.add(mergedRex);
                mergedVariableInfo.add(currentVar);
                mergedFields.add(currentField);
                name2InputRex.remove(name);
                name2InputVar.remove(name);
                name2InputField.remove(name);
            } else {
                mergedNodes.add(currentRex);
                mergedVariableInfo.add(currentVar);
                mergedFields.add(currentField);
            }
        }

        mergedNodes.addAll(name2InputRex.values());
        mergedVariableInfo.addAll(name2InputVar.values());
        mergedFields.addAll(name2InputField.values());

        RelDataType mergedType;
        if (typeName == SqlTypeName.VERTEX) {
            mergedType =
                VertexRecordType.createVertexType(mergedFields, builder.getTypeFactory());
        } else {
            mergedType =
                EdgeRecordType.createEdgeType(mergedFields, builder.getTypeFactory());
        }

        return new RexObjectConstruct(mergedType, mergedNodes, mergedVariableInfo);
    }

    private RexNode mergeRexNode(RexNode inputRex, RexNode currentRex, int pathIndex, String variableName) {
        return currentRex.accept(new RexShuttle() {

            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                if (fieldAccess.getReferenceExpr() instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) fieldAccess.getReferenceExpr();
                    if (inputRef.getIndex() == pathIndex && variableName.equals(fieldAccess.getField().getName())) {
                        return inputRex;
                    }
                }
                return fieldAccess;
            }
        });
    }

    @Override
    protected @Nonnull String computeDigest(boolean withType) {
        final StringBuilder sb = new StringBuilder(op.getName());
        sb.append("{");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = getType().getFieldList().get(i).getName();
            sb.append(name).append(":").append(operands.get(i));
        }
        sb.append("}");

        if (withType) {
            sb.append(":");
            sb.append(type.getFullTypeString());
        }
        return sb.toString();
    }

    private static SqlOperator createConstructOperator(RelDataType dataType) {
        SqlIdentifier[] keyNodes = new SqlIdentifier[dataType.getFieldCount()];
        int c = 0;
        for (RelDataTypeField field : dataType.getFieldList()) {
            keyNodes[c++] = new SqlIdentifier(field.getName(), SqlParserPos.ZERO);
        }
        // Construct RexObjectConstruct for dynamic field append expression.
        return dataType.getSqlTypeName() == SqlTypeName.VERTEX
            ? new SqlVertexConstructOperator(keyNodes)
            : new SqlEdgeConstructOperator(keyNodes);
    }

    private static List<RexNode> reOrderOperands(RelDataType type, List<? extends RexNode> operands) {
        List<RexNode> reOrderNodes = new ArrayList<>();
        switch (type.getSqlTypeName()) {
            case VERTEX:
                int idIndex = -1;
                int labelIndex = -1;
                for (int i = 0; i < operands.size(); i++) {
                    RexNode operand = operands.get(i);
                    if (operand.getType() instanceof MetaFieldType) {
                        MetaFieldType metaFieldType = (MetaFieldType) operand.getType();
                        if (metaFieldType.getMetaField() == MetaField.VERTEX_ID) {
                            idIndex = i;
                        } else if (metaFieldType.getMetaField() == MetaField.VERTEX_TYPE) {
                            labelIndex = i;
                        }
                    }
                }
                assert idIndex >= 0 : "Id field must defined in the vertex constructor";
                reOrderNodes.add(operands.get(idIndex));
                if (labelIndex >= 0) {
                    reOrderNodes.add(operands.get(labelIndex));
                } else {
                    reOrderNodes.add(DEFAULT_LABEL_VALUE);
                }

                for (int i = 0; i < operands.size(); i++) {
                    if (i != idIndex && i != labelIndex) {
                        reOrderNodes.add(operands.get(i));
                    }
                }
                return reOrderNodes;
            case EDGE:
                int srcIdIndex = -1;
                int targetIdIndex = -1;
                int edgeLabelIndex = -1;
                int tsIndex = -1;
                for (int i = 0; i < operands.size(); i++) {
                    RexNode operand = operands.get(i);
                    if (operand.getType() instanceof MetaFieldType) {
                        MetaFieldType metaFieldType = (MetaFieldType) operand.getType();
                        switch (metaFieldType.getMetaField()) {
                            case EDGE_SRC_ID:
                                srcIdIndex = i;
                                break;
                            case EDGE_TARGET_ID:
                                targetIdIndex = i;
                                break;
                            case EDGE_TYPE:
                                edgeLabelIndex = i;
                                break;
                            case EDGE_TS:
                                tsIndex = i;
                                break;
                            default:
                        }
                    }
                }
                assert srcIdIndex >= 0 : "Source id field must defined in edge constructor";
                assert targetIdIndex >= 0 : "Target id field must defined in edge constructor";
                reOrderNodes.add(operands.get(srcIdIndex));
                reOrderNodes.add(operands.get(targetIdIndex));
                if (edgeLabelIndex >= 0) {
                    reOrderNodes.add(operands.get(edgeLabelIndex));
                } else {
                    reOrderNodes.add(DEFAULT_LABEL_VALUE);
                }
                if (tsIndex >= 0) {
                    reOrderNodes.add(operands.get(tsIndex));
                }
                for (int i = 0; i < operands.size(); i++) {
                    if (i != srcIdIndex && i != targetIdIndex && i != edgeLabelIndex && i != tsIndex) {
                        reOrderNodes.add(operands.get(i));
                    }
                }
                return reOrderNodes;
            default:
                throw new IllegalArgumentException("Illegal type name: " + type.getSqlTypeName()
                    + " for Object constructor.");
        }
    }

    public static class VariableInfo {

        /**
         * Whether it is a global variable.
         */
        private final boolean isGlobal;

        /**
         * Variable name.
         */
        private final String name;

        public VariableInfo(boolean isGlobal, String name) {
            this.isGlobal = isGlobal;
            this.name = name;
        }

        public boolean isGlobal() {
            return isGlobal;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VariableInfo)) {
                return false;
            }
            VariableInfo that = (VariableInfo) o;
            return isGlobal == that.isGlobal && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isGlobal, name);
        }

        @Override
        public String toString() {
            return "VariableInfo{"
                + "isGlobal=" + isGlobal
                + ", name='" + name + '\''
                + '}';
        }
    }
}
