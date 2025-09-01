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
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlAlterGraph extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlAlterGraph",
        SqlKind.ALTER_GRAPH);

    private SqlIdentifier alterName;
    private SqlNodeList vertices;
    private SqlNodeList edges;

    public SqlAlterGraph(SqlParserPos pos, SqlIdentifier alterName,
                         SqlNodeList vertices, SqlNodeList edges) {
        super(pos);
        this.alterName = alterName;
        this.vertices = vertices;
        this.edges = edges;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.alterName = (SqlIdentifier) operand;
                break;
            case 1:
                this.vertices = (SqlNodeList) operand;
                break;
            case 2:
                this.edges = (SqlNodeList) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getVertices(), getEdges());
    }

    @Override
    public SqlOperator getOperator() {
        return this.OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        for (SqlNode addV : vertices) {
            writer.keyword("alter");
            writer.keyword("graph");
            alterName.unparse(writer, 0, 0);
            writer.keyword("add");
            addV.unparse(writer, 0, 0);
        }
        for (SqlNode addE : edges) {
            writer.keyword("alter");
            writer.keyword("graph");
            alterName.unparse(writer, 0, 0);
            writer.keyword("add");
            addE.unparse(writer, 0, 0);
        }
    }

    public SqlIdentifier getName() {
        return alterName;
    }

    public SqlNodeList getVertices() {
        return vertices;
    }

    public SqlNodeList getEdges() {
        return edges;
    }

    public void validate() throws ValidationException {
    }
}
