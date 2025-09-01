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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

public class ExpressionUtil {

    public static String showExpression(RexNode node,
                                        List<RexNode> calcExps,
                                        RelDataType inputRowType) {
        return node.accept(new RexNodeExpressionVisitor(calcExps, inputRowType));
    }

    static class RexNodeExpressionVisitor implements RexVisitor<String> {

        private final List<RexNode> calcExps;
        private final RelDataType inputRowType;

        public RexNodeExpressionVisitor(List<RexNode> localExps,
                                        RelDataType inputRowType) {
            this.calcExps = localExps;
            this.inputRowType = inputRowType;
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();

            return inputRowType.getFieldList().get(index).getName();
        }

        @Override
        public String visitLocalRef(RexLocalRef localRef) {
            int index = localRef.getIndex();
            RexNode node = calcExps.get(index);
            return node.accept(this);
        }

        @Override
        public String visitLiteral(RexLiteral literal) {
            if (literal.getType().getSqlTypeName() == SqlTypeName.CHAR
                || literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                NlsString nlsString = (NlsString) literal.getValue();
                if (nlsString != null) {
                    return StringLiteralUtil.escapeSQLString(String.valueOf(nlsString.getValue()));
                } else {
                    return "null";
                }
            }
            return String.valueOf(literal.getValue());
        }

        @Override
        public String visitCall(RexCall call) {

            String operatorName = call.getOperator().getName();
            List<RexNode> operands = call.getOperands();

            List<String> operandExpressions = new ArrayList<>();
            for (RexNode operand : operands) {
                operandExpressions.add(operand.accept(this));
            }
            StringBuilder sb = new StringBuilder();

            switch (operatorName) {
                case "+":
                case "-":
                case "=":
                case "<>":
                case "%":
                case "*":
                case "/":
                case "<":
                case ">":
                case "<=":
                case ">=":
                case "OR":
                case "AND":
                case "||":

                    sb.append("(");

                    for (int i = 0; i < operandExpressions.size(); i++) {
                        if (i > 0) {
                            sb.append(" ").append(operatorName).append(" ");
                        }
                        sb.append(operandExpressions.get(i));
                    }
                    sb.append(")");
                    return sb.toString();

                case "CASE":
                    sb.append("CASE ");
                    for (int i = 0; i < operandExpressions.size() - 1; i += 2) {
                        sb.append("WHEN ").append(operandExpressions.get(i))
                            .append(" THEN ").append(operandExpressions.get(i + 1));
                    }
                    sb.append(" ELSE ")
                        .append(operandExpressions.get(operandExpressions.size() - 1))
                        .append(" END");
                    return sb.toString();

                case "CAST":
                    RelDataType targetType = call.getType();
                    return operatorName + "(" + operandExpressions.get(0) + " as " + targetType.getSqlTypeName() + ")";
                case "ITEM":
                    return operandExpressions.get(0) + "[" + operandExpressions.get(1) + "]";
                default:
                    return operatorName + "(" + Joiner.on(',').join(operandExpressions) + ")";
            }
        }

        @Override
        public String visitOver(RexOver over) {
            return over.toString();
        }

        @Override
        public String visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable.toString();
        }

        @Override
        public String visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam.toString();
        }

        @Override
        public String visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef.toString();
        }

        @Override
        public String visitFieldAccess(RexFieldAccess fieldAccess) {
            String refExpr = fieldAccess.getReferenceExpr().accept(this);
            return refExpr + "." + fieldAccess.getField().getName();
        }

        @Override
        public String visitSubQuery(RexSubQuery subQuery) {
            return subQuery.toString();
        }

        @Override
        public String visitTableInputRef(RexTableInputRef fieldRef) {
            return fieldRef.toString();
        }

        @Override
        public String visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return fieldRef.toString();
        }

        @Override
        public String visitOther(RexNode other) {
            return other.toString();
        }
    }
}
