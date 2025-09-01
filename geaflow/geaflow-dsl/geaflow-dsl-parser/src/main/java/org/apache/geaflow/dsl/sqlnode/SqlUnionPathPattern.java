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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.operator.SqlPathPatternOperator;

public class SqlUnionPathPattern extends SqlCall {

    private SqlNode left;

    private SqlNode right;

    private SqlLiteral unionType;

    public SqlUnionPathPattern(SqlParserPos pos, SqlNode left, SqlNode right, boolean distinct) {
        super(pos);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
        if (distinct) {
            this.unionType = SqlLiteral.createSymbol(UnionPathPatternType.UNION_DISTINCT, pos);
        } else {
            this.unionType = SqlLiteral.createSymbol(UnionPathPatternType.UNION_ALL, pos);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return SqlPathPatternOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(left, right, unionType);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.left = operand;
                break;
            case 1:
                this.right = operand;
                break;
            case 2:
                this.unionType = (SqlLiteral) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        left.unparse(writer, leftPrec, rightPrec);
        switch (getUnionPathPatternType()) {
            case UNION_DISTINCT:
                writer.print(" | ");
                break;
            case UNION_ALL:
                writer.print(" |+| ");
                break;
            default:
                throw new GeaFlowDSLException("Unknown union path pattern type: "
                    + getUnionPathPatternType());
        }
        right.unparse(writer, leftPrec, rightPrec);
    }

    public SqlNode getLeft() {
        return left;
    }

    public SqlNode getRight() {
        return right;
    }

    public boolean isDistinct() {
        return getUnionPathPatternType() == UnionPathPatternType.UNION_DISTINCT;
    }

    public boolean isUnionAll() {
        return getUnionPathPatternType() == UnionPathPatternType.UNION_ALL;
    }

    public final UnionPathPatternType getUnionPathPatternType() {
        return unionType.symbolValue(UnionPathPatternType.class);
    }

    public enum UnionPathPatternType {
        UNION_DISTINCT,
        UNION_ALL
    }
}
