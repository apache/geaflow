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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlTableColumn.ColumnCategory;
import org.apache.geaflow.dsl.util.GQLEdgeConstraint;

public class SqlEdge extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlEdge",
        SqlKind.OTHER_DDL);
    private SqlIdentifier name;
    private SqlNodeList columns;
    private SqlNodeList constraints;

    public SqlEdge(SqlParserPos pos, SqlIdentifier name, SqlNodeList columns, SqlNodeList constraints) {
        super(pos);
        this.name = name;
        this.columns = columns;
        this.constraints = constraints;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.columns = (SqlNodeList) operand;
                break;
            case 2:
                this.constraints = (SqlNodeList) operand;
                break;
            default:
                throw new IndexOutOfBoundsException("current index " + i + " out of range " + 3);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getColumns(), getConstraints());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("edge");
        name.unparse(writer, 0, 0);
        writer.print("(");
        writer.newlineAndIndent();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.print(",\n");
            }
            columns.get(i).unparse(writer, 0, 0);
        }
        writer.newlineAndIndent();
        writer.print(")");
        if (constraints != null && constraints.size() > 0) {
            for (int i = 0; i < constraints.size(); i++) {
                if (i > 0) {
                    writer.print("\n");
                }
                constraints.get(i).unparse(writer, 0, 0);
            }
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    public SqlNodeList getConstraints() {
        return constraints;
    }

    public void validate() {
        SqlIdentifier sourceVertex = null;
        SqlIdentifier targetVertex = null;
        for (Object c : columns) {
            SqlTableColumn column = (SqlTableColumn) c;
            column.validate();
            if (column.getCategory() == ColumnCategory.SOURCE_ID) {
                assert sourceVertex == null : "Duplicated source id field.";
                sourceVertex = column.getTypeFrom();
            } else if (column.getCategory() == ColumnCategory.DESTINATION_ID) {
                assert targetVertex == null : "Duplicated destination id field.";
                targetVertex = column.getTypeFrom();
            }
        }
        if (sourceVertex != null && targetVertex != null) {
            this.constraints = new SqlNodeList(Collections.singletonList(new GQLEdgeConstraint(
                new SqlNodeList(Collections.singletonList(sourceVertex), getParserPosition()),
                new SqlNodeList(Collections.singletonList(targetVertex), getParserPosition()),
                getParserPosition()
            )), getParserPosition());
        } else if (sourceVertex == null && targetVertex != null) {
            throw new GeaFlowDSLException("The vertex source id from in edge '{}' should be set.",
                getName().getSimple());
        } else if (sourceVertex != null) {
            throw new GeaFlowDSLException("The vertex target id from in edge '{}' should be set.",
                getName().getSimple());
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        this.validate();
        super.validate(validator, scope);
    }
}
