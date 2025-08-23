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
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlBasicQueryOperator;

public class SqlPathPatternSubQuery extends SqlCall {

    private static final SqlOperator OPERATOR = SqlBasicQueryOperator.of("PathPatternSubQuery");

    private SqlPathPattern pathPattern;

    private SqlNode returnValue;

    public SqlPathPatternSubQuery(SqlPathPattern pathPattern,
                                  SqlNode returnValue, SqlParserPos pos) {
        super(pos);
        this.pathPattern = Objects.requireNonNull(pathPattern);
        this.returnValue = returnValue;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.pathPattern = (SqlPathPattern) operand;
                break;
            case 1:
                this.returnValue = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        pathPattern.unparse(writer, 0, 0);
        if (returnValue != null) {
            writer.print("=>");
            returnValue.unparse(writer, 0, 0);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(pathPattern, returnValue);
    }

    public SqlPathPattern getPathPattern() {
        return pathPattern;
    }

    public SqlNode getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(SqlNode returnValue) {
        this.returnValue = returnValue;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_PATH_PATTERN_SUB_QUERY;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        SqlValidatorNamespace namespace = validator.getNamespace(this);
        namespace.validate(validator.getUnknownType());
    }
}
