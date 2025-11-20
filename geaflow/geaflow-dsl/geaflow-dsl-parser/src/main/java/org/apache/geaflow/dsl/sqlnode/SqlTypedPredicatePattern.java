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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SQL node for ISO-GQL Value Type Predicate function calls.
 *
 * <p>Supports two forms:
 * - TYPED(expression, typename): Check if expression is of specified type
 * - NOT_TYPED(expression, typename): Check if expression is NOT of specified type
 *
 * <p>Examples:
 *   TYPED(n.age, INTEGER)       -> true if n.age is an integer
 *   NOT_TYPED(n.data, STRING)   -> true if n.data is not a string
 *
 * <p>This is a simplified function-call approach that works seamlessly with
 * Calcite's function call mechanism without modifying the parser grammar.
 */
public class SqlTypedPredicatePattern extends SqlCall {

    private final SqlNode expression;
    private final SqlIdentifier typeName;
    private final boolean isNot;
    private final SqlOperator operator;

    /**
     * Creates a new SqlTypedPredicatePattern for type predicate function calls.
     *
     * @param pos The position in the source code
     * @param operator The SQL operator (TYPED or NOT_TYPED function)
     * @param expression The expression to check
     * @param typeName The target type name
     * @param isNot True if this is NOT_TYPED, false if TYPED
     */
    public SqlTypedPredicatePattern(
            SqlParserPos pos,
            SqlOperator operator,
            SqlNode expression,
            SqlIdentifier typeName,
            boolean isNot) {
        super(pos);
        this.operator = operator;
        this.expression = expression;
        this.typeName = typeName;
        this.isNot = isNot;
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return java.util.Arrays.asList(expression, typeName);
    }

    /**
     * Get the expression being type-checked.
     *
     * @return the expression
     */
    public SqlNode getExpression() {
        return expression;
    }

    /**
     * Get the target type name.
     *
     * @return the type name identifier
     */
    public SqlIdentifier getTypeName() {
        return typeName;
    }

    /**
     * Check if this is IS NOT TYPED (vs IS TYPED).
     *
     * @return true if IS NOT TYPED, false if IS TYPED
     */
    public boolean isNot() {
        return isNot;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Unparse as function call: TYPED(expr, type) or NOT_TYPED(expr, type)
        writer.keyword(isNot ? "NOT_TYPED" : "TYPED");
        writer.print("(");
        expression.unparse(writer, 0, 0);
        writer.print(",");
        writer.print(" ");
        typeName.unparse(writer, 0, 0);
        writer.print(")");
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER;
    }
}


