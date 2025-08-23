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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlVertex extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlVertex",
        SqlKind.OTHER_DDL);
    private SqlIdentifier name;
    private SqlNodeList columns;

    public SqlVertex(SqlParserPos pos, SqlIdentifier name, SqlNodeList columns) {
        super(pos);
        this.name = name;
        this.columns = columns;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.columns = (SqlNodeList) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getColumns());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("vertex");
        name.unparse(writer, 0, 0);
        writer.print("(");
        writer.newlineAndIndent();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.print(",\n");
            }
            columns.get(i).unparse(writer, 0, 0);
        }
        writer.newlineAndIndent();
        writer.print(")");
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumns() {
        return columns;
    }

}
