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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.operator.SqlSameOperator;

/**
 * SqlNode representing the ISO-GQL SAME predicate function.
 *
 * <p>The SAME predicate checks if multiple element references point to the same
 * graph element (identity check, not value equality).
 *
 * <p>Syntax: SAME(element_ref1, element_ref2 [, element_ref3, ...])
 *
 * <p>Example:
 * <pre>
 * MATCH (a:Person)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)
 * WHERE SAME(a, c)
 * RETURN a.name, b.name;
 * </pre>
 *
 * <p>This returns triangular paths where the start and end vertices are the same element.
 *
 * <p>Implements ISO/IEC 39075:2024 Section 19.12 (SAME predicate).
 */
public class SqlSameCall extends SqlCall {

    private final List<SqlNode> operands;

    /**
     * Creates a SqlSameCall.
     *
     * @param pos Parser position
     * @param operands List of element reference expressions (must be 2 or more)
     */
    public SqlSameCall(SqlParserPos pos, List<SqlNode> operands) {
        super(pos);
        this.operands = Objects.requireNonNull(operands, "operands");

        // ISO-GQL requires at least 2 arguments
        if (operands.size() < 2) {
            throw new IllegalArgumentException(
                "SAME predicate requires at least 2 arguments, got: " + operands.size());
        }
    }

    @Override
    public SqlOperator getOperator() {
        return SqlSameOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return operands;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // Validation will be handled by GQLSameValidator
        // This just validates the syntax is correct
        for (SqlNode operand : operands) {
            operand.validate(validator, scope);
        }
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i < 0 || i >= operands.size()) {
            throw new IllegalArgumentException("Invalid operand index: " + i);
        }
        operands.set(i, operand);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("SAME");
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");

        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                writer.sep(",");
            }
            operands.get(i).unparse(writer, 0, 0);
        }

        writer.endList(frame);
    }

    /**
     * Returns the number of operands (element references) in this SAME call.
     */
    public int getOperandCount() {
        return operands.size();
    }

    /**
     * Returns the operand at the specified index.
     */
    public SqlNode getOperand(int index) {
        return operands.get(index);
    }
}
