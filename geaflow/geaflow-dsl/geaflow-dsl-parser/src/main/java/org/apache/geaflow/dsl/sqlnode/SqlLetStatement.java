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

package org.apache.geaflow.dsl.sqlnode;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.operator.SqlLetOperator;

public class SqlLetStatement extends SqlCall {

    /**
     * The input node for this statement.
     */
    private SqlNode from;

    private SqlIdentifier leftVar;

    private SqlNode expression;

    private final boolean isGlobal;

    public SqlLetStatement(SqlParserPos pos, SqlNode from, SqlIdentifier leftVar,
                           SqlNode expression, boolean isGlobal) {
        super(pos);
        this.from = from;
        this.leftVar = Objects.requireNonNull(leftVar);
        this.expression = Objects.requireNonNull(expression);
        this.isGlobal = isGlobal;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlLetOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(from, leftVar, expression);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (leftVar.names.size() != 2) {
            throw new GeaFlowDSLException(leftVar.getParserPosition(),
                "Illegal left variable field size: {}", leftVar.names.size());
        }
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.from = operand;
                break;
            case 1:
                this.leftVar = (SqlIdentifier) operand;
                break;
            case 2:
                this.expression = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (from != null) {
            from.unparse(writer, 0, 0);
            writer.print("\n");
        }
        writer.keyword("LET");
        if (isGlobal) {
            writer.keyword("GLOBAL");
        }
        leftVar.unparse(writer, 0, 0);
        writer.keyword("=");
        expression.unparse(writer, 0, 0);
    }

    public SqlNode getFrom() {
        return from;
    }

    public SqlIdentifier getLeftVar() {
        return leftVar;
    }

    public void setLeftVar(SqlIdentifier leftVar) {
        this.leftVar = leftVar;
    }

    public String getLeftLabel() {
        return leftVar.names.get(0);
    }

    public String getLeftField() {
        return leftVar.names.get(1);
    }

    public SqlNode getExpression() {
        return expression;
    }

    public void setExpression(SqlNode expression) {
        this.expression = expression;
    }

    public boolean isGlobal() {
        return isGlobal;
    }
}
