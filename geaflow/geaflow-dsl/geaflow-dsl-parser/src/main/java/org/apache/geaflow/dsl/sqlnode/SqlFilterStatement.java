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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.operator.SqlFilterOperator;

public class SqlFilterStatement extends SqlCall {

    private SqlNode from;

    private SqlNode condition;

    public SqlFilterStatement(SqlParserPos pos, SqlNode from, SqlNode condition) {
        super(pos);
        this.from = from;
        this.condition = condition;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlFilterOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(getFrom(), getCondition());
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_FILTER;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.from = operand;
                break;
            case 1:
                this.condition = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (from != null) {
            from.unparse(writer, 0, 0);
        }
        if (condition != null) {
            if (from != null) {
                writer.keyword("THEN");
                writer.newlineAndIndent();
            }
            writer.keyword("FILTER");
            condition.unparse(writer, 0, 0);
        }
        writer.newlineAndIndent();
    }

    public final SqlNode getFrom() {
        return this.from;
    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    public final SqlNode getCondition() {
        return this.condition;
    }

    public void setCondition(SqlNode condition) {
        this.condition = condition;
    }

}
