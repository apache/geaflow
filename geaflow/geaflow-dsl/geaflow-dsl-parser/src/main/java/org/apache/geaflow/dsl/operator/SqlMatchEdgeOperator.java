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

package org.apache.geaflow.dsl.operator;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;

public class SqlMatchEdgeOperator extends SqlOperator {

    public static final SqlMatchEdgeOperator INSTANCE = new SqlMatchEdgeOperator();

    private SqlMatchEdgeOperator() {
        super("MatchEdge", SqlKind.GQL_MATCH_EDGE, 2, true, ReturnTypes.SCOPE, null, null);
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
        String directionName = operands[3].toString();

        return new SqlMatchEdge(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1],
            (SqlNodeList) operands[2], operands[3],
            EdgeDirection.of(directionName), 1, 1);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    @Override
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        call.unparse(writer, leftPrec, rightPrec);
    }
}
