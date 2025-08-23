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

package org.apache.geaflow.dsl.runtime.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.ExpressionTranslator;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.apache.geaflow.state.data.TimeRange;

public class FilterPushDownUtil {

    /**
     * Find timestamp range condition in the expression.
     */
    public static List<TimeRange> findTsRange(RexNode rexNode,
                                              EdgeRecordType edgeRecordType) {
        int tsFieldIndex = edgeRecordType.getTimestampIndex();
        if (tsFieldIndex < 0) {
            return new ArrayList<>();
        }
        return rexNode.accept(new RexVisitor<List<TimeRange>>() {
            @Override
            public List<TimeRange> visitInputRef(RexInputRef rexInputRef) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitLocalRef(RexLocalRef rexLocalRef) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitLiteral(RexLiteral rexLiteral) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitCall(RexCall call) {
                SqlKind kind = call.getKind();
                switch (kind) {
                    case BETWEEN:
                        RexNode node = call.operands.get(0);
                        RexNode leftValue = call.operands.get(1);
                        RexNode rightValue = call.operands.get(2);
                        if (node instanceof RexFieldAccess) {
                            if (tsFieldIndex == ((RexFieldAccess) node).getField().getIndex()) {
                                Long leftTsValue = null;
                                Long rightTsValue = null;
                                if (GQLRexUtil.isLiteralOrParameter(leftValue, true)) {
                                    leftTsValue = toTsLongValue(leftValue);
                                }
                                if (GQLRexUtil.isLiteralOrParameter(rightValue, true)) {
                                    rightTsValue = toTsLongValue(rightValue);
                                }
                                TimeRange result = TimeRange.of(leftTsValue == null ? Long.MIN_VALUE : leftTsValue, rightTsValue == null ? Long.MAX_VALUE : rightTsValue
                                );
                                return new ArrayList<>(Collections.singletonList(result));
                            }
                        }
                        return new ArrayList<>();
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case EQUALS:
                        RexNode rangeValue = null;
                        RexNode left = call.operands.get(0);
                        RexNode right = call.operands.get(1);
                        SqlKind realKind = kind;
                        if (left instanceof RexFieldAccess && GQLRexUtil.isLiteralOrParameter(right,
                            true)) {
                            RexFieldAccess leftAccess = (RexFieldAccess) left;
                            int index = leftAccess.getField().getIndex();
                            if (tsFieldIndex == index) {
                                rangeValue = right;
                            }
                        } else if (right instanceof RexFieldAccess && GQLRexUtil.isLiteralOrParameter(left, true)) {
                            RexFieldAccess rightAccess = (RexFieldAccess) right;
                            int index = rightAccess.getField().getIndex();
                            if (tsFieldIndex == index) {
                                switch (kind) {
                                    case LESS_THAN:
                                        realKind = SqlKind.GREATER_THAN;
                                        break;
                                    case LESS_THAN_OR_EQUAL:
                                        realKind = SqlKind.GREATER_THAN_OR_EQUAL;
                                        break;
                                    case GREATER_THAN:
                                        realKind = SqlKind.LESS_THAN;
                                        break;
                                    case GREATER_THAN_OR_EQUAL:
                                        realKind = SqlKind.LESS_THAN_OR_EQUAL;
                                        break;
                                    default:
                                }
                                rangeValue = left;
                            }
                        }
                        if (rangeValue != null) {
                            Long ts = toTsLongValue(rangeValue);
                            if (ts == null) {
                                return new ArrayList<>();
                            }
                            switch (realKind) {
                                case LESS_THAN:
                                    return Collections.singletonList(TimeRange.of(Long.MIN_VALUE, ts));
                                case LESS_THAN_OR_EQUAL:
                                    if (ts < Long.MAX_VALUE) {
                                        return Collections.singletonList(TimeRange.of(Long.MIN_VALUE, ts + 1));
                                    }
                                    return Collections.singletonList(TimeRange.of(Long.MIN_VALUE, Long.MAX_VALUE));
                                case GREATER_THAN:
                                    if (ts < Long.MAX_VALUE) {
                                        return Collections.singletonList(TimeRange.of(ts + 1, Long.MAX_VALUE));
                                    }
                                    return Collections.singletonList(TimeRange.of(Long.MAX_VALUE, Long.MAX_VALUE));
                                case GREATER_THAN_OR_EQUAL:
                                    return Collections.singletonList(TimeRange.of(ts, Long.MAX_VALUE));
                                case EQUALS:
                                    if (ts < Long.MAX_VALUE) {
                                        return Collections.singletonList(TimeRange.of(ts, ts + 1));
                                    } else {
                                        return Collections.singletonList(TimeRange.of(ts, Long.MAX_VALUE));
                                    }
                                default:
                            }
                        }
                        return new ArrayList<>();
                    case AND:
                        return call.operands.stream()
                            .map(operand -> operand.accept(this))
                            .filter(list -> list != null && !list.isEmpty())
                            .reduce(FilterPushDownUtil::timeRangeIntersection)
                            .orElse(new ArrayList<>());
                    case OR:
                        return call.operands.stream()
                            .map(operand -> operand.accept(this))
                            .filter(list -> list != null && !list.isEmpty())
                            .reduce(FilterPushDownUtil::timeRangeUnion)
                            .orElse(new ArrayList<>());
                    default:
                        return new ArrayList<>();
                }
            }

            @Override
            public List<TimeRange> visitOver(RexOver rexOver) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitDynamicParam(RexDynamicParam rexDynamicParam) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitRangeRef(RexRangeRef rexRangeRef) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitFieldAccess(RexFieldAccess rexFieldAccess) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitSubQuery(RexSubQuery rexSubQuery) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitTableInputRef(RexTableInputRef rexTableInputRef) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
                return new ArrayList<>();
            }

            @Override
            public List<TimeRange> visitOther(RexNode other) {
                return new ArrayList<>();
            }
        });
    }

    private static Long toTsLongValue(RexNode rangeValue) {
        List<RexNode> nonLiteralLeafNodes = GQLRexUtil.collect(rangeValue,
            child -> !(child instanceof RexCall) && !(child instanceof RexLiteral));
        ExpressionTranslator translator = ExpressionTranslator.of(null);
        Expression expression = translator.translate(rangeValue);
        if (nonLiteralLeafNodes.isEmpty()) { // all the leaf node is constant.
            Object constantValue = expression.evaluate(null);
            assert constantValue instanceof Number : "Not Number timestamp range.";
            return ((Number) constantValue).longValue();
        }
        //todo Parameter timestamp range not support push down currently.
        return null;
    }

    public static List<TimeRange> timeRangeIntersection(final List<TimeRange> ranges,
                                                        final List<TimeRange> others) {
        List<TimeRange> tmpList = new ArrayList<>();
        for (TimeRange a : ranges) {
            for (TimeRange b : ranges) {
                long maxStart = Math.max(a.getStart(), b.getStart());
                long maxEnd = Math.max(a.getEnd(), b.getEnd());
                TimeRange range = TimeRange.of(maxStart, maxEnd);
                tmpList.add(range);
            }
        }
        return new ArrayList<>(mergeTsRanges(tmpList));
    }

    public static List<TimeRange> mergeTsRanges(List<TimeRange> list) {
        list.sort(Comparator.comparing(TimeRange::getStart));
        for (int i = 0; i < list.size() - 1; i++) {
            TimeRange outer;
            TimeRange inner;
            for (int j = i + 1; j < list.size(); j++) {
                outer = list.get(i);
                inner = list.get(j);
                long end = list.get(i).getEnd();
                long end2 = inner.getEnd();
                if (end >= inner.getStart() && end <= end2) {
                    TimeRange tmpRange = TimeRange.of(outer.getStart(), end2);
                    list.set(i, tmpRange);
                    list.remove(j);
                    j--;
                } else if (end >= end2 || outer.getStart() == end2) {
                    list.remove(j--);
                }
            }
        }
        return list;
    }

    public static List<TimeRange> timeRangeUnion(final List<TimeRange> ranges, final List<TimeRange> others) {
        List<TimeRange> tmpList = new ArrayList<>();
        tmpList.addAll(ranges);
        tmpList.addAll(others);
        return new ArrayList<>(mergeTsRanges(tmpList));
    }
}
