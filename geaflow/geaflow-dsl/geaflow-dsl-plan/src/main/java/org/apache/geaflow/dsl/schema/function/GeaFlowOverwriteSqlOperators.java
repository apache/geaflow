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

package org.apache.geaflow.dsl.schema.function;

import static org.apache.geaflow.dsl.util.FunctionUtil.getSqlReturnTypeInference;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class GeaFlowOverwriteSqlOperators {

    public static final SqlBinaryOperator PLUS =
        new SqlMonotonicBinaryOperator(
            "+",
            SqlKind.PLUS,
            40,
            true,
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "plus"),
            InferTypes.FIRST_KNOWN,
            OperandTypes.PLUS_OPERATOR);

    public static final SqlBinaryOperator MINUS =
        new SqlMonotonicBinaryOperator(
            "-",
            SqlKind.MINUS,
            40,
            true,
            // Same type inference strategy as sum
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "plus"),
            InferTypes.FIRST_KNOWN,
            OperandTypes.MINUS_OPERATOR);

    public static final SqlBinaryOperator DIVIDE =
        new SqlBinaryOperator(
            "/",
            SqlKind.DIVIDE,
            60,
            true,
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "divide"),
            InferTypes.FIRST_KNOWN,
            OperandTypes.DIVISION_OPERATOR);

    public static final SqlBinaryOperator MULTIPLY =
        new SqlMonotonicBinaryOperator(
            "*",
            SqlKind.TIMES,
            60,
            true,
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "times"),
            InferTypes.FIRST_KNOWN,
            OperandTypes.MULTIPLY_OPERATOR);

    public static final SqlFunction MOD =
        // Return type is same as divisor (2nd operand)
        // SQL2003 Part2 Section 6.27, Syntax Rules 9
        new SqlFunction(
            "MOD",
            SqlKind.MOD,
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "mod"),
            null,
            OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
            SqlFunctionCategory.NUMERIC);

    public static final SqlBinaryOperator PERCENT_REMAINDER =
        new SqlBinaryOperator(
            "%",
            SqlKind.MOD,
            60,
            true,
            getSqlReturnTypeInference(GeaFlowBuiltinFunctions.class, "mod"),
            null,
            OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC);

    public static final SqlFunction SIGN =
        new SqlFunction(
            "SIGN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.NUMERIC);
}
