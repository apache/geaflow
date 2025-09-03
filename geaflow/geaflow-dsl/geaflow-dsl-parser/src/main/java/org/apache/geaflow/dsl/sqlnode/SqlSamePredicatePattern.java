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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;


/**
 * SQL node representing a same predicate pattern in GQL.
 * This node represents a pattern where two path patterns share a common predicate condition.
 *
 * <p>Example: MATCH (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 25)
 *
 * <p>The left and right path patterns must satisfy the same predicate condition.
 */
public class SqlSamePredicatePattern extends SqlCall {

    /**
     * Operator for same predicate pattern.
     */
    public static final SqlOperator OPERATOR = new SqlSpecialOperator("SAME_PREDICATE", SqlKind.OTHER);

    /**
     * Left path pattern.
     */
    private final SqlNode left;

    /**
     * Right path pattern.
     */
    private final SqlNode right;

    /**
     * Shared predicate condition that must be satisfied by both path patterns.
     */
    private final SqlNode predicate;

    /**
     * Whether to use distinct semantics (true) or union all (false).
     */
    private final boolean isDistinct;

    /**
     * Constructor for SqlSamePredicatePattern.
     *
     * @param pos parser position
     * @param left left path pattern
     * @param right right path pattern
     * @param predicate shared predicate condition
     * @param isDistinct whether to use distinct semantics
     */
    public SqlSamePredicatePattern(SqlParserPos pos, SqlNode left, SqlNode right,
                                   SqlNode predicate, boolean isDistinct) {
        super(pos);
        this.left = Objects.requireNonNull(left, "left path pattern cannot be null");
        this.right = Objects.requireNonNull(right, "right path pattern cannot be null");
        this.predicate = Objects.requireNonNull(predicate, "predicate cannot be null");
        this.isDistinct = isDistinct;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(left, right, predicate);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                // left path pattern
                throw new UnsupportedOperationException("Cannot modify left path pattern after creation");
            case 1:
                // right path pattern
                throw new UnsupportedOperationException("Cannot modify right path pattern after creation");
            case 2:
                // predicate
                throw new UnsupportedOperationException("Cannot modify predicate after creation");
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    /**
     * Get the left path pattern.
     *
     * @return left path pattern
     */
    public SqlNode getLeft() {
        return left;
    }

    /**
     * Get the right path pattern.
     *
     * @return right path pattern
     */
    public SqlNode getRight() {
        return right;
    }

    /**
     * Get the shared predicate condition.
     *
     * @return predicate condition
     */
    public SqlNode getPredicate() {
        return predicate;
    }

    /**
     * Check if this pattern uses distinct semantics.
     *
     * @return true if distinct, false if union all
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * Check if this pattern uses union all semantics.
     *
     * @return true if union all, false if distinct
     */
    public boolean isUnionAll() {
        return !isDistinct;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Unparse left path pattern
        left.unparse(writer, leftPrec, rightPrec);

        // Unparse union operator
        if (isDistinct) {
            writer.print(" | ");
        } else {
            writer.print(" |+| ");
        }

        // Unparse right path pattern
        right.unparse(writer, leftPrec, rightPrec);

        // Unparse WHERE SAME clause
        writer.keyword("WHERE");
        writer.keyword("SAME");
        writer.keyword("(");
        predicate.unparse(writer, leftPrec, rightPrec);
        writer.keyword(")");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // Validate left path pattern
        validator.validateQuery(left, scope, validator.getUnknownType());

        // Validate right path pattern
        validator.validateQuery(right, scope, validator.getUnknownType());

        // Validate predicate expression by calling validate on the predicate itself
        predicate.validate(validator, scope);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SqlSamePredicatePattern(");
        sb.append("left=").append(left);
        sb.append(", right=").append(right);
        sb.append(", predicate=").append(predicate);
        sb.append(", distinct=").append(isDistinct);
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlSamePredicatePattern that = (SqlSamePredicatePattern) obj;
        return isDistinct == that.isDistinct
               && Objects.equals(left, that.left)
               && Objects.equals(right, that.right)
               && Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, predicate, isDistinct);
    }
}
