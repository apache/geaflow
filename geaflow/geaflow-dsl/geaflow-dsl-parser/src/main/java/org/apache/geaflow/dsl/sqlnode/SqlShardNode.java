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

public class SqlShardNode extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlShardNode", SqlKind.OTHER_DDL);

    private SqlIdentifier type;

    private SqlNumericLiteral shardCount;

    public SqlShardNode(SqlParserPos pos, SqlIdentifier type, SqlNumericLiteral shardCount) {
        super(pos);
        this.type = type;
        this.shardCount = shardCount;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.type = (SqlIdentifier) operand;
                break;
            case 1:
                this.shardCount = (SqlNumericLiteral) operand;
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
        return ImmutableList.of(type, shardCount);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("Shard As ");
        type.unparse(writer, 0, 0);
        writer.print("(");
        shardCount.unparse(writer, 0, 0);
        writer.print(")");
    }
}
