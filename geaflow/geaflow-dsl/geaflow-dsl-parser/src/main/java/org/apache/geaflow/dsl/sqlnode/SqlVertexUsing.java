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

public class SqlVertexUsing extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlVertexUsing",
        SqlKind.OTHER_DDL);

    private SqlIdentifier name;
    private SqlIdentifier usingTableName;
    private SqlIdentifier id;

    public SqlVertexUsing(SqlParserPos pos, SqlIdentifier name,
                          SqlIdentifier usingTableName,
                          SqlIdentifier id) {
        super(pos);
        this.name = name;
        this.usingTableName = usingTableName;
        this.id = id;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.usingTableName = (SqlIdentifier) operand;
                break;
            case 2:
                this.id = (SqlIdentifier) operand;
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
        return ImmutableList.of(getName(), getUsingTableName(), getId());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("VERTEX");
        name.unparse(writer, 0, 0);
        writer.keyword("USING");
        usingTableName.unparse(writer, 0, 0);
        writer.keyword("WITH");
        writer.keyword("ID");
        writer.print("(");
        id.unparse(writer, 0, 0);
        writer.print(")");

    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlIdentifier getUsingTableName() {
        return usingTableName;
    }

    public SqlIdentifier getId() {
        return id;
    }
}
