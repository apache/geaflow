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
import org.apache.geaflow.dsl.sqlnode.SqlMatchPattern;

public class SqlMatchPatternOperator extends SqlOperator {

    public static final SqlMatchPatternOperator INSTANCE = new SqlMatchPatternOperator();

    private SqlMatchPatternOperator() {
        super("MatchPattern", SqlKind.OTHER, 2, true,
                ReturnTypes.SCOPE, null, null);
    }

    @Override
    public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
        return new SqlMatchPattern(pos, operands[0], (SqlNodeList) operands[1], operands[2],
                (SqlNodeList) operands[3], operands[4], false);
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
