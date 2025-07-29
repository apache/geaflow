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
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.util.SqlNodeUtil;

public class SqlCreateGraph extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlCreateGraph",
        SqlKind.CREATE_GRAPH);

    private SqlIdentifier name;
    private SqlNodeList vertices;
    private SqlNodeList edges;
    private SqlNodeList properties;
    private final boolean isTemporary;

    public SqlCreateGraph(SqlParserPos pos, boolean isTemporary, boolean ifNotExists,
                          SqlIdentifier name, SqlNodeList vertices,
                          SqlNodeList edges, SqlNodeList properties) {
        super(OPERATOR, pos, false, ifNotExists);
        this.name = name;
        this.vertices = vertices;
        this.edges = edges;
        this.properties = properties;
        this.isTemporary = isTemporary;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.vertices = (SqlNodeList) operand;
                break;
            case 2:
                this.edges = (SqlNodeList) operand;
                break;
            case 3:
                this.properties = (SqlNodeList) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getVertices(), getEdges(), getProperties());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("GRAPH");
        if (super.ifNotExists) {
            writer.keyword("IF");
            writer.keyword("NOT");
            writer.keyword("EXISTS");
        }
        name.unparse(writer, 0, 0);
        writer.print("(");
        writer.newlineAndIndent();
        SqlNodeUtil.unparseNodeList(writer, vertices, ",");
        writer.print(",");
        writer.newlineAndIndent();
        SqlNodeUtil.unparseNodeList(writer, edges, ",");
        writer.newlineAndIndent();
        writer.print(")");
        if (properties != null && properties.size() > 0) {
            writer.keyword("WITH");
            writer.print("(");
            writer.newlineAndIndent();
            SqlNodeUtil.unparseNodeList(writer, properties, ",");
            writer.newlineAndIndent();
            writer.print(")");
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getVertices() {
        return vertices;
    }

    public SqlNodeList getEdges() {
        return edges;
    }

    public SqlNodeList getProperties() {
        return properties;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }
}
