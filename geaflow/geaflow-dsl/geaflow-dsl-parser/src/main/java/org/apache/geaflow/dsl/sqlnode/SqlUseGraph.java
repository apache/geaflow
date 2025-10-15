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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlUseGraph extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlUseGraph",
        SqlKind.USE_GRAPH);

    private SqlIdentifier graph;

    public SqlUseGraph(SqlParserPos pos, SqlIdentifier graph) {
        super(pos);
        this.graph = Objects.requireNonNull(graph);
    }

    @Override
    protected void unparseAlterOperation(SqlWriter sqlWriter, int leftPrec, int rightPrec) {
        sqlWriter.keyword("USE");
        sqlWriter.keyword("GRAPH");
        graph.unparse(sqlWriter, 0, 0);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(graph);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i == 0) {
            this.graph = (SqlIdentifier) operand;
        } else {
            throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    public String getGraph() {
        if (graph.names.size() == 1) {
            return graph.getSimple();
        }
        throw new IllegalArgumentException("Illegal graph name size: " + graph.names.size());
    }
}
