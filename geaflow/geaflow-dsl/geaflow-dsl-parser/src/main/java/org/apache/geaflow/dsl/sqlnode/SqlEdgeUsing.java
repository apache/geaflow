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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlEdgeUsing extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlEdgeUsing",
        SqlKind.OTHER_DDL);

    private SqlIdentifier name;
    private SqlIdentifier usingTableName;
    private SqlIdentifier sourceId;
    private SqlIdentifier targetId;
    private SqlIdentifier timeField;
    private SqlNodeList constraints;

    public SqlEdgeUsing(SqlParserPos pos, SqlIdentifier name,
                        SqlIdentifier usingTableName,
                        SqlIdentifier sourceId,
                        SqlIdentifier targetId,
                        SqlIdentifier timeField,
                        SqlNodeList constraints) {
        super(pos);
        this.name = name;
        this.usingTableName = usingTableName;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.timeField = timeField;
        this.constraints = constraints;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.usingTableName = (SqlIdentifier) operand;
                break;
            case 2:
                this.sourceId = (SqlIdentifier) operand;
                break;
            case 3:
                this.targetId = (SqlIdentifier) operand;
                break;
            case 4:
                this.timeField = (SqlIdentifier) operand;
                break;
            case 5:
                this.constraints = (SqlNodeList) operand;
                break;
            default:
                throw new AssertionError();
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(getName(), getUsingTableName(), getSourceId(), getTargetId(),
            getTimeField(), getConstraints());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EDGE");
        name.unparse(writer, 0, 0);
        writer.keyword("USING");
        usingTableName.unparse(writer, 0, 0);
        writer.keyword("WITH");
        writer.keyword("ID");
        writer.print("(");
        sourceId.unparse(writer, 0, 0);
        writer.print(",");
        targetId.unparse(writer, 0, 0);
        writer.print(")");
        if (timeField != null) {
            writer.print(",");
            writer.keyword("TIMESTAMP");
            writer.print("(");
            timeField.unparse(writer, 0, 0);
            writer.print(")");
        }
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

    public SqlIdentifier getUsingTableName() {
        return usingTableName;
    }

    public SqlIdentifier getSourceId() {
        return sourceId;
    }

    public SqlIdentifier getTargetId() {
        return targetId;
    }

    public SqlIdentifier getTimeField() {
        return timeField;
    }

    public SqlNodeList getConstraints() {
        return constraints;
    }

}
