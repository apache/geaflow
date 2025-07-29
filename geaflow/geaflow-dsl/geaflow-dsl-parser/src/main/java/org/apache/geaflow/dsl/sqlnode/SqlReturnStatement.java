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
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlReturnOperator;
import org.apache.geaflow.dsl.util.GQLReturnKeyword;

public class SqlReturnStatement extends SqlCall {

    private SqlNode from;

    private SqlNodeList gqlReturnKeywordList;

    private SqlNodeList returnList;

    private SqlNodeList groupBy;

    private SqlNodeList orderBy;

    private SqlNode offset;

    private SqlNode fetch;

    public SqlReturnStatement(SqlParserPos pos, SqlNodeList gqlReturnKeywordList, SqlNode from,
                              SqlNodeList returnList, SqlNodeList groupBy, SqlNodeList orderBy,
                              SqlNode offset, SqlNode fetch) {
        super(pos);
        this.gqlReturnKeywordList = gqlReturnKeywordList;
        this.from = from;
        this.returnList = returnList;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.offset = offset;
        this.fetch = fetch;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlReturnOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getGQLReturnKeywordList(), getFrom(), getReturnList(),
            getGroupBy(), getOrderBy(), getOffset(), getFetch());
    }

    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.gqlReturnKeywordList = (SqlNodeList) operand;
                break;
            case 1:
                this.from = operand;
                break;
            case 2:
                this.returnList = (SqlNodeList) operand;
                break;
            case 3:
                this.groupBy = (SqlNodeList) operand;
                break;
            case 4:
                this.orderBy = (SqlNodeList) operand;
                break;
            case 5:
                this.offset = operand;
                break;
            case 6:
                this.fetch = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }

    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    public void setReturnList(SqlNodeList returnList) {
        this.returnList = returnList;
    }

    public void setGroupBy(SqlNodeList groupBy) {
        this.groupBy = groupBy;
    }

    public final SqlNodeList getOrderList() {
        return this.orderBy;
    }

    public void setOrderBy(SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    public void setOffset(SqlNode offset) {
        this.offset = offset;
    }

    public void setFetch(SqlNode fetch) {
        this.fetch = fetch;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!writer.inQuery()) {
            Frame frame = writer.startList(FrameTypeEnum.SUB_QUERY, "(", ")");
            unparseCall(writer, 0, 0);
            writer.endList(frame);
        } else {
            unparseCall(writer, leftPrec, rightPrec);
        }
    }

    private void unparseCall(SqlWriter writer, int leftPrec, int rightPrec) {
        if (from != null) {
            from.unparse(writer, leftPrec, rightPrec);
        }
        if (returnList != null && returnList.size() > 0) {
            writer.keyword("RETURN");
            if (gqlReturnKeywordList != null) {
                for (int i = 0; i < gqlReturnKeywordList.size(); i++) {
                    SqlNode keyword = gqlReturnKeywordList.get(i);
                    writer.print(" ");
                    keyword.unparse(writer, leftPrec, rightPrec);
                }
                writer.print(" ");
            }
            for (int i = 0; i < returnList.size(); i++) {
                SqlNode label = returnList.get(i);
                if (i > 0) {
                    writer.print(",");
                }
                label.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
        }
        if (groupBy != null && groupBy.size() > 0) {
            writer.keyword("GROUP BY");
            for (int i = 0; i < groupBy.size(); i++) {
                SqlNode label = groupBy.get(i);
                if (i > 0) {
                    writer.print(",");
                }
                label.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
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

        if (fetch != null) {
            writer.keyword("LIMIT");
            fetch.unparse(writer, leftPrec, rightPrec);
        }

        if (offset != null) {
            writer.keyword("OFFSET");
            offset.unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_RETURN;
    }

    public SqlNode getGQLReturnKeywordList() {
        return gqlReturnKeywordList;
    }

    public SqlNode getFrom() {
        return from;
    }

    public SqlNodeList getReturnList() {
        return returnList;
    }

    public SqlNodeList getGroupBy() {
        return groupBy;
    }

    public SqlNodeList getOrderBy() {
        return orderBy;
    }

    public SqlNode getOffset() {
        return offset;
    }

    public SqlNode getFetch() {
        return fetch;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    public final boolean isDistinct() {
        return getModifierNode(GQLReturnKeyword.DISTINCT) != null;
    }

    public final SqlNode getModifierNode(GQLReturnKeyword modifier) {
        if (gqlReturnKeywordList != null) {
            for (SqlNode keyword : gqlReturnKeywordList) {
                GQLReturnKeyword keyword2 =
                    ((SqlLiteral) keyword).symbolValue(GQLReturnKeyword.class);
                if (keyword2 == modifier) {
                    return keyword;
                }
            }
        }
        return null;
    }
}
