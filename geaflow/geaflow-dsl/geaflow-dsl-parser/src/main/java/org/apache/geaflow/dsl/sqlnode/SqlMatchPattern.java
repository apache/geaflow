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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlMatchPatternOperator;

public class SqlMatchPattern extends SqlCall {

    private SqlNode from;

    private SqlNodeList pathPatterns;

    private SqlNode where;

    private SqlNodeList orderBy;

    private SqlNode limit;

    private boolean optional;

    public SqlMatchPattern(SqlParserPos pos, SqlNode from, SqlNodeList pathPatterns,
            SqlNode where, SqlNodeList orderBy, SqlNode limit, boolean optional) {
        super(pos);
        this.from = from;
        this.pathPatterns = pathPatterns;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.optional = optional;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlMatchPatternOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getFrom(), getPathPatterns(), getWhere(),
                getOrderBy(), getLimit());
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_PATTERN;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    public SqlNode getFrom() {
        return from;
    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    public SqlNodeList getOrderBy() {
        return orderBy;
    }

    public SqlNode getLimit() {
        return limit;
    }

    public void setOrderBy(SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    public void setLimit(SqlNode limit) {
        this.limit = limit;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.from = operand;
                break;
            case 1:
                this.pathPatterns = (SqlNodeList) operand;
                break;
            case 2:
                this.where = operand;
                break;
            case 3:
                this.orderBy = (SqlNodeList) operand;
                break;
            case 4:
                this.limit = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("Match");
        if (pathPatterns != null) {
            for (int i = 0; i < pathPatterns.size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                pathPatterns.get(i).unparse(writer, leftPrec, rightPrec);
                writer.newlineAndIndent();
            }
        }
        if (where != null) {
            writer.keyword("WHERE");
            where.unparse(writer, 0, 0);
        }
        if (orderBy != null && orderBy.size() > 0) {
            writer.keyword("ORDER BY");
            for (int i = 0; i < orderBy.size(); i++) {
                SqlNode label = orderBy.get(i);
                if (i > 0) {
                    writer.print(",");
                }
                label.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
        }
        if (limit != null) {
            writer.keyword("LIMIT");
            limit.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNodeList getPathPatterns() {
        return pathPatterns;
    }

    public SqlNode getWhere() {
        return where;
    }

    public final boolean isDistinct() {
        return false;
    }

    public void setWhere(SqlNode where) {
        this.where = where;
    }

    public boolean isSinglePattern() {
        return pathPatterns.size() == 1 && pathPatterns.get(0) instanceof SqlPathPattern;
    }

    public boolean isOptional() {
        return optional;
    }
}
