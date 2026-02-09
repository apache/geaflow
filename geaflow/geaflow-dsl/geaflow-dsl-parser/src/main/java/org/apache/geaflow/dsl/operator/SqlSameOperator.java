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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.geaflow.dsl.sqlnode.SqlSameCall;

/**
 * SqlOperator for the ISO-GQL SAME predicate function.
 *
 * <p>This operator represents the SAME function which checks element identity.
 *
 * <p>Syntax: SAME(element1, element2, ...)
 *
 * <p>Returns: BOOLEAN - TRUE if all element references point to the same element,
 * FALSE otherwise.
 *
 * <p>Implements ISO/IEC 39075:2024 Section 19.12.
 */
public class SqlSameOperator extends SqlFunction {

    public static final SqlSameOperator INSTANCE = new SqlSameOperator();

    private SqlSameOperator() {
        super(
            "SAME",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            // At least 2 operands, all must be of comparable types
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
        return new SqlSameCall(pos, java.util.Arrays.asList(operands));
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
