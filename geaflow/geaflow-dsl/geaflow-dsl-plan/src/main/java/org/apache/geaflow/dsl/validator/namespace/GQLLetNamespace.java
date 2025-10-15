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

package org.apache.geaflow.dsl.validator.namespace;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlLetStatement;
import org.apache.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.geaflow.dsl.validator.QueryNodeContext;
import org.apache.geaflow.dsl.validator.scope.GQLPathPatternScope;

public class GQLLetNamespace extends GQLBaseNamespace {

    private final SqlLetStatement letStatement;

    public GQLLetNamespace(SqlValidatorImpl validator, SqlLetStatement letStatement) {
        super(validator, letStatement);
        this.letStatement = letStatement;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        if (letStatement.getFrom() != null) {
            SqlValidatorNamespace fromNs = validator.getNamespace(letStatement.getFrom());
            fromNs.validate(targetRowType);

            SqlValidatorScope scope = ((GQLValidatorImpl) validator).getScopes(letStatement);

            RelDataType fromType = fromNs.getType();
            assert fromType instanceof PathRecordType;

            PathRecordType inputPathType = (PathRecordType) fromType;
            String leftLabel = letStatement.getLeftLabel();

            RelDataTypeField labelField = inputPathType.getField(leftLabel,
                isCaseSensitive(), false);
            if (labelField == null) {
                throw new GeaFlowDSLException(letStatement.getLeftVar().getParserPosition().toString(),
                    "Left label: {} is not exists in input path fields: {}.", leftLabel, fromType.getFieldNames());
            }
            SqlNode expression = letStatement.getExpression();
            expression = validator.expand(expression, scope);
            letStatement.setExpression(expression);

            expression.validate(validator, scope);
            ((GQLValidatorImpl) validator).inferUnknownTypes(validator.getUnknownType(), scope, expression);
            RelDataType expressionType = validator.deriveType(scope, expression);
            // set let expression type nullable
            expressionType = validator.getTypeFactory().createTypeWithNullability(expressionType, true);
            RelDataType labelType = labelField.getType();
            // check exist field type.
            RelDataTypeField existField = labelType.getField(letStatement.getLeftField(), isCaseSensitive(), false);
            if (existField != null && !SqlTypeUtil.canCastFrom(existField.getType(), expressionType, true)) {
                throw new GeaFlowDSLException(letStatement.getParserPosition().toString(),
                    "Let statement cannot assign from {} to {}", expressionType, existField.getType().getSqlTypeName());
            }
            RelDataType newLabelType = labelType;
            if (existField == null) { // first define this let variable.
                if (labelType.getSqlTypeName() == SqlTypeName.VERTEX) {
                    newLabelType = ((VertexRecordType) labelType).add(letStatement.getLeftField(),
                        expressionType, isCaseSensitive());
                } else if (labelType.getSqlTypeName() == SqlTypeName.EDGE) {
                    newLabelType = ((EdgeRecordType) labelType).add(letStatement.getLeftField(),
                        expressionType, isCaseSensitive());
                } else {
                    throw new IllegalArgumentException("Illegal labelType: " + labelType);
                }
            }
            GraphRecordType graphRecordType =
                GQLPathPatternScope.findCurrentGraphType(getValidator(), scope);
            assert graphRecordType != null;
            // Modify the query node level graph schema if it is a global vertex field modify
            if (letStatement.isGlobal()) {
                if (labelType.getSqlTypeName() != SqlTypeName.VERTEX) {
                    throw new GeaFlowDSLException(letStatement.getParserPosition(),
                        "Only vertex support global variable");
                }
                QueryNodeContext nodeContext = getValidator().getCurrentQueryNodeContext();
                GraphRecordType newGraphType = graphRecordType.addVertexField(letStatement.getLeftField(),
                    expressionType);
                // add to the node context
                nodeContext.addModifyGraph(newGraphType);
                // add to the validator and used by the GQLToRelConverter.
                getValidator().addModifyGraphType(letStatement, newGraphType);
            } else {
                getValidator().addModifyGraphType(letStatement, graphRecordType);
            }
            // replace the type for left label.
            return inputPathType.copy(labelField.getIndex(), newLabelType);
        }
        throw new GeaFlowDSLException(letStatement.getParserPosition(), "Let without from is not support");
    }

    @Override
    public SqlNode getNode() {
        return letStatement;
    }
}
