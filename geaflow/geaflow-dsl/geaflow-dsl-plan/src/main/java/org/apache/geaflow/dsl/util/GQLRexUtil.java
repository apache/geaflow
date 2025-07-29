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

package org.apache.geaflow.dsl.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions;

public class GQLRexUtil {

    private static class RexCollectVisitor<O extends RexNode> implements RexVisitor<List<O>> {

        private final Predicate<RexNode> condition;

        public RexCollectVisitor(Predicate<RexNode> condition) {
            this.condition = condition;
        }

        @Override
        public List<O> visitInputRef(RexInputRef inputRef) {
            if (condition.test(inputRef)) {
                return (List<O>) Collections.singletonList(inputRef);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitLocalRef(RexLocalRef localRef) {
            if (condition.test(localRef)) {
                return (List<O>) Collections.singletonList(localRef);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitLiteral(RexLiteral literal) {
            if (condition.test(literal)) {
                return (List<O>) Collections.singletonList(literal);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitCall(RexCall call) {
            List<RexNode> childNodes = call.operands.stream()
                .flatMap(operand -> operand.accept(this).stream())
                .collect(Collectors.toList());
            if (condition.test(call)) {
                List<RexNode> nodes = new ArrayList<>(childNodes);
                nodes.add(call);
                return (List<O>) nodes;
            }
            return (List<O>) childNodes;
        }

        @Override
        public List<O> visitOver(RexOver over) {
            if (condition.test(over)) {
                return (List<O>) Collections.singletonList(over);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitCorrelVariable(RexCorrelVariable correlVariable) {
            if (condition.test(correlVariable)) {
                return (List<O>) Collections.singletonList(correlVariable);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitDynamicParam(RexDynamicParam dynamicParam) {
            if (condition.test(dynamicParam)) {
                return (List<O>) Collections.singletonList(dynamicParam);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitRangeRef(RexRangeRef rangeRef) {
            if (condition.test(rangeRef)) {
                return (List<O>) Collections.singletonList(rangeRef);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitFieldAccess(RexFieldAccess fieldAccess) {
            List<RexNode> collects = new ArrayList<>(fieldAccess.getReferenceExpr().accept(this));
            if (condition.test(fieldAccess)) {
                collects.add(fieldAccess);
                return (List<O>) collects;
            }
            return (List<O>) collects;
        }

        @Override
        public List<O> visitSubQuery(RexSubQuery subQuery) {
            ResultRexShuffle<O> resultRexShuffle = new ResultRexShuffle<>(this);
            subQuery.rel.accept(resultRexShuffle);
            return resultRexShuffle.getResult();
        }

        @Override
        public List<O> visitTableInputRef(RexTableInputRef fieldRef) {
            if (condition.test(fieldRef)) {
                return (List<O>) Collections.singletonList(fieldRef);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            if (condition.test(fieldRef)) {
                return (List<O>) Collections.singletonList(fieldRef);
            }
            return Collections.emptyList();
        }

        @Override
        public List<O> visitOther(RexNode other) {
            if (condition.test(other)) {
                return (List<O>) Collections.singletonList(other);
            }
            return Collections.emptyList();
        }
    }

    /**
     * Collect sub-node for {@link RexNode} which satisfy the condition.
     *
     * @param rexNode   The rex-node to collect.
     * @param condition The collect condition.
     * @return The sub-node list which satisfy the condition.
     */
    @SuppressWarnings("unchecked")
    public static <O extends RexNode> List<O> collect(RexNode rexNode, Predicate<RexNode> condition) {
        return rexNode.accept(new RexCollectVisitor<>(condition));
    }

    public static <O extends RexNode> List<O> collect(RelNode node, Predicate<RexNode> condition) {
        RexCollectVisitor<O> collectVisitor = new RexCollectVisitor<>(condition);
        ResultRexShuffle<O> resultShuffle = new ResultRexShuffle<>(collectVisitor);
        node.accept(resultShuffle);
        return resultShuffle.getResult();
    }

    /**
     * Whether the rex-node contains specified kind of child node.
     */
    public static boolean contain(RexNode rexNode, Class<? extends RexNode> targetNodeClass) {
        return !collect(rexNode, operand -> operand.getClass() == targetNodeClass).isEmpty();
    }

    /**
     * Replace the sub-node of the {@link RexNode} to the new sub-node defined by the replace function.
     *
     * @param rexNode   The rex-node to replace.
     * @param replaceFn The replace function which mapping the old rex-node to the new rex-node.
     * @return The replaced rex-node.
     */
    public static RexNode replace(RexNode rexNode, Function<RexNode, RexNode> replaceFn) {
        return rexNode.accept(new RexVisitor<RexNode>() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                return replaceFn.apply(inputRef);
            }

            @Override
            public RexNode visitLocalRef(RexLocalRef localRef) {
                return replaceFn.apply(localRef);
            }

            @Override
            public RexNode visitLiteral(RexLiteral literal) {
                return replaceFn.apply(literal);
            }

            @Override
            public RexNode visitCall(RexCall call) {
                List<RexNode> newOperands = call.operands.stream()
                    .map(operand -> operand.accept(this))
                    .collect(Collectors.toList());

                RexNode newCall = call.clone(call.getType(), newOperands);
                return replaceFn.apply(newCall);
            }

            @Override
            public RexNode visitOver(RexOver over) {
                return replaceFn.apply(over);
            }

            @Override
            public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
                return replaceFn.apply(correlVariable);
            }

            @Override
            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                return replaceFn.apply(dynamicParam);
            }

            @Override
            public RexNode visitRangeRef(RexRangeRef rangeRef) {
                return replaceFn.apply(rangeRef);
            }

            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                return replaceFn.apply(fieldAccess);
            }

            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                return replaceFn.apply(subQuery);
            }

            @Override
            public RexNode visitTableInputRef(RexTableInputRef fieldRef) {
                return replaceFn.apply(fieldRef);
            }

            @Override
            public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
                return replaceFn.apply(fieldRef);
            }

            @Override
            public RexNode visitOther(RexNode other) {
                return replaceFn.apply(other);
            }
        });
    }

    /**
     * Find vertex ids in the expression.
     * e.g. for "a.id = '1' or a.id = '2'", Set("1", "2") will return.
     *
     * @param rexNode          The expression.
     * @param vertexRecordType The input vertex type for the expression.
     * @return The id literals referred by the expression.
     */
    public static Set<RexNode> findVertexIds(RexNode rexNode, VertexRecordType vertexRecordType) {
        return rexNode.accept(new RexVisitor<Set<RexNode>>() {
            @Override
            public Set<RexNode> visitInputRef(RexInputRef rexInputRef) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitLocalRef(RexLocalRef rexLocalRef) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitLiteral(RexLiteral rexLiteral) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitCall(RexCall call) {
                SqlKind kind = call.getKind();
                switch (kind) {
                    case EQUALS:
                        RexNode idValue = null;
                        RexNode left = call.operands.get(0);
                        RexNode right = call.operands.get(1);
                        if (isIdField(vertexRecordType, left) && isLiteralOrParameter(right, true)) {
                            idValue = right;
                        } else if (isIdField(vertexRecordType, right) && isLiteralOrParameter(left, true)) {
                            idValue = left;
                        }
                        if (idValue != null) {
                            return Sets.newHashSet(idValue);
                        } else {
                            return new HashSet<>();
                        }
                    case AND:
                        return call.operands.stream()
                            .map(operand -> operand.accept(this))
                            .filter(set -> !set.isEmpty())
                            .reduce(Sets::intersection)
                            .orElse(new HashSet<>());
                    case OR:
                        return call.operands.stream()
                            .map(operand -> operand.accept(this))
                            .reduce((a, b) -> {
                                if (a.isEmpty() || b.isEmpty()) {
                                    // all child should be id condition, else return empty.
                                    return Sets.newHashSet();
                                } else {
                                    return Sets.union(a, b);
                                }
                            })
                            .orElse(new HashSet<>());
                    case CAST:
                        return call.operands.get(0).accept(this);
                    default:
                        return new HashSet<>();
                }
            }

            @Override
            public Set<RexNode> visitOver(RexOver rexOver) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitDynamicParam(RexDynamicParam rexDynamicParam) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitRangeRef(RexRangeRef rexRangeRef) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitFieldAccess(RexFieldAccess rexFieldAccess) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitSubQuery(RexSubQuery rexSubQuery) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitTableInputRef(RexTableInputRef rexTableInputRef) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
                return new HashSet<>();
            }

            @Override
            public Set<RexNode> visitOther(RexNode other) {
                return new HashSet<>();
            }
        });
    }


    public static RexNode swapReverseEdgeRef(RexNode rexNode, String reverseEdgeName,
                                             RexBuilder rexBuilder) {
        return GQLRexUtil.replace(rexNode,
            node -> {
                if (node instanceof RexFieldAccess
                    && ((RexFieldAccess) node).getReferenceExpr() instanceof PathInputRef) {
                    RexFieldAccess fieldAccess = (RexFieldAccess) node;
                    PathInputRef pathInputRef = (PathInputRef) fieldAccess.getReferenceExpr();
                    if (pathInputRef.getLabel().equals(reverseEdgeName)
                        && fieldAccess.getType() instanceof MetaFieldType) {
                        if (((MetaFieldType) fieldAccess.getType()).getMetaField()
                            .equals(MetaField.EDGE_SRC_ID)) {
                            return rexBuilder.makeFieldAccess(pathInputRef,
                                ((EdgeRecordType) pathInputRef.getType()).getTargetIdField()
                                    .getIndex());
                        } else if (((MetaFieldType) fieldAccess.getType()).getMetaField()
                            .equals(MetaField.EDGE_TARGET_ID)) {
                            return rexBuilder.makeFieldAccess(pathInputRef,
                                ((EdgeRecordType) pathInputRef.getType()).getSrcIdField()
                                    .getIndex());
                        }
                    }
                }
                return node;
            });
    }

    public static RexNode removeIdCondition(RexNode condition, VertexRecordType vertexRecordType) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            switch (call.getKind()) {
                case EQUALS:
                    RexNode left = call.operands.get(0);
                    RexNode right = call.operands.get(1);
                    if (isIdField(vertexRecordType, left) && isLiteralOrParameter(right, true)) {
                        return null;
                    }
                    if (isIdField(vertexRecordType, right) && isLiteralOrParameter(left, true)) {
                        return null;
                    }
                    break;
                case AND:
                    List<RexNode> filterOperands = call.operands.stream()
                        .filter(operand -> removeIdCondition(operand, vertexRecordType) != null)
                        .collect(Collectors.toList());
                    if (filterOperands.size() == 0) {
                        return null;
                    } else if (filterOperands.size() == 1) {
                        return filterOperands.get(0);
                    }
                    return call.clone(call.getType(), filterOperands);
                case OR:
                    boolean allRemove =
                        call.operands.stream().allMatch(operand -> removeIdCondition(operand,
                            vertexRecordType) == null);
                    if (allRemove) {
                        return null;
                    }
                    break;
                case CAST:
                    RexNode newOperand = removeIdCondition(call.operands.get(0), vertexRecordType);
                    if (newOperand == null) {
                        return null;
                    }
                    return call.clone(call.getType(), Collections.singletonList(newOperand));
                default:
            }
        }
        return condition;
    }

    private static boolean isIdField(VertexRecordType vertexRecordType, RexNode node) {
        if (node instanceof RexFieldAccess) {
            int index = ((RexFieldAccess) node).getField().getIndex();
            return vertexRecordType.isId(index);
        }
        return false;
    }

    public static Object getLiteralValue(RexNode node) {
        SqlKind kind = node.getKind();
        if (kind == SqlKind.LITERAL) {
            RexLiteral literal = (RexLiteral) node;
            return getLiteralValue(literal);
        } else if (kind == SqlKind.CAST) {
            RexCall cast = (RexCall) node;
            Object value = getLiteralValue(cast.operands.get(0));
            IType<?> targetType = SqlTypeUtil.convertType(cast.getType());
            return TypeCastUtil.cast(value, targetType);
        }
        throw new IllegalArgumentException("RexNode: " + node + " is not a literal");
    }

    public static Object getLiteralValue(RexLiteral literal) {
        if (literal == null) {
            return null;
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        Object value = literal.getValue();
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case BOOLEAN:
                return Boolean.class.cast(value);

            case TINYINT:
                return ((BigDecimal) literal.getValue()).byteValue();
            case SMALLINT:
                return ((BigDecimal) literal.getValue()).shortValue();
            case INTEGER:
                return ((BigDecimal) literal.getValue()).intValue();
            case BIGINT:
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_DAY:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR:
                return ((BigDecimal) literal.getValue()).longValue();

            case FLOAT:
            case DOUBLE:
            case REAL:
                return ((BigDecimal) literal.getValue()).doubleValue();
            case DECIMAL:
                return literal.getValue();

            case CHAR:
            case VARCHAR:
                Preconditions
                    .checkArgument(literal.getValue() instanceof NlsString,
                        "literal create type char/varchar must be NlsString type");
                return StringLiteralUtil
                    .unescapeSQLString("\"" + RexLiteral.stringValue(literal) + "\"");
            case SYMBOL:
                Preconditions.checkArgument(value instanceof Enum,
                    "literal create type symbol must be Enum type");
                if (value instanceof TimeUnitRange) {
                    return ((TimeUnitRange) value).startUnit.multiplier.intValue();
                } else if (value instanceof SqlTrimFunction.Flag) {
                    SqlTrimFunction.Flag flag = (Flag) value;
                    switch (flag) {
                        case BOTH:
                            return GeaFlowBuiltinFunctions.TRIM_BOTH;
                        case LEADING:
                            return GeaFlowBuiltinFunctions.TRIM_LEFT;
                        case TRAILING:
                            return GeaFlowBuiltinFunctions.TRIM_RIGHT;
                        default:
                            throw new IllegalArgumentException("illegal trim flag: " + flag);
                    }
                }
                break;
            case DATE:
                return java.sql.Date.valueOf(literal.toString());
            case TIME:
                return Time.valueOf(literal.toString());
            case TIMESTAMP:
                return Timestamp.valueOf(literal.toString());
            case BINARY:
            case VARBINARY:
                return byte[].class.cast(literal.getValue());
            default:
                throw new GeaFlowDSLException("Not support type:" + typeName);
        }
        throw new GeaFlowDSLException("Not support type:" + typeName);
    }

    public static RexNode toPathInputRefForWhere(RelDataTypeField pathField, RexNode where) {
        RexBuilder builder = new RexBuilder(GQLJavaTypeFactory.create());
        return where.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                PathInputRef pathInputRef = new PathInputRef(pathField.getName(),
                    pathField.getIndex(), pathField.getType());
                return builder.makeFieldAccess(pathInputRef, inputRef.getIndex());
            }
        });
    }

    public static RexLiteral createString(String value) {
        RexBuilder rexBuilder = new RexBuilder(GQLJavaTypeFactory.create());
        return rexBuilder.makeLiteral(value);
    }

    public static boolean isLiteralOrParameter(RexNode rexNode, boolean allowCast) {
        if (rexNode.getKind() == SqlKind.CAST && allowCast) {
            return isLiteralOrParameter(((RexCall) rexNode).operands.get(0), true);
        }
        return !contain(rexNode, RexInputRef.class) && !contain(rexNode, RexFieldAccess.class);
    }

    public static boolean isVertexIdFieldAccess(RexNode rexNode) {
        if (rexNode instanceof RexFieldAccess) {
            RexFieldAccess op = (RexFieldAccess) rexNode;
            if (op.getReferenceExpr() instanceof PathInputRef
                && op.getType() instanceof MetaFieldType) {
                MetaFieldType opType = (MetaFieldType) op.getType();
                return opType.getMetaField() == MetaField.VERTEX_ID;
            }
        }
        return false;
    }

    public static class ResultRexShuffle<O> extends RexShuttle {

        private final RexVisitor<List<O>> baseVisitor;

        private final List<O> result = new ArrayList<>();

        public ResultRexShuffle(RexVisitor<List<O>> baseVisitor) {
            this.baseVisitor = baseVisitor;
        }

        public List<O> getResult() {
            return result;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            List<O> visitResults = baseVisitor.visitInputRef(inputRef);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return inputRef;
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef localRef) {
            List<O> visitResults = baseVisitor.visitLocalRef(localRef);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return localRef;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            List<O> visitResults = baseVisitor.visitLiteral(literal);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return literal;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<O> visitResults = baseVisitor.visitCall(call);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return call;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            List<O> visitResults = baseVisitor.visitOver(over);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return over;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            List<O> visitResults = baseVisitor.visitCorrelVariable(correlVariable);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return correlVariable;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            List<O> visitResults = baseVisitor.visitDynamicParam(dynamicParam);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return dynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            List<O> visitResults = baseVisitor.visitRangeRef(rangeRef);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return rangeRef;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            List<O> visitResults = baseVisitor.visitFieldAccess(fieldAccess);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return fieldAccess;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            List<O> visitResults = baseVisitor.visitSubQuery(subQuery);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return subQuery;
        }

        @Override
        public RexNode visitTableInputRef(RexTableInputRef fieldRef) {
            List<O> visitResults = baseVisitor.visitTableInputRef(fieldRef);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return fieldRef;
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            List<O> visitResults = baseVisitor.visitPatternFieldRef(fieldRef);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return fieldRef;
        }

        @Override
        public RexNode visitOther(RexNode other) {
            List<O> visitResults = baseVisitor.visitOther(other);
            if (visitResults != null) {
                result.addAll(visitResults);
            }
            return other;
        }
    }

    public static RexNode and(List<RexNode> conditions, RexBuilder builder) {
        if (conditions == null) {
            return null;
        }
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        return builder.makeCall(SqlStdOperatorTable.AND, conditions);
    }
}
