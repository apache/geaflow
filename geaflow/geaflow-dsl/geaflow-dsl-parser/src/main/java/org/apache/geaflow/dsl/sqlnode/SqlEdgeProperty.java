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

public class SqlEdgeProperty extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlEdgeProperty",
        SqlKind.OTHER_DDL);

    private SqlIdentifier sourceId;
    private SqlIdentifier targetId;
    private SqlNode type;
    private SqlNode direct;
    private SqlNode timeField;

    public SqlEdgeProperty(SqlParserPos pos, SqlIdentifier sourceId, SqlIdentifier targetId,
                           SqlNode type, SqlNode direct, SqlNode timeField) {
        super(pos);
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.type = type;
        this.direct = direct;
        this.timeField = timeField;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.sourceId = (SqlIdentifier) operand;
                break;
            case 1:
                this.targetId = (SqlIdentifier) operand;
                break;
            case 2:
                this.type = operand;
                break;
            case 3:
                this.direct = operand;
                break;
            case 4:
                this.timeField = operand;
                break;
            default:
                break;
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(sourceId, targetId, type, direct, timeField);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("edge");
        writer.keyword("prop");
        writer.print("(");
        sourceId.unparse(writer, 0, 0);
        writer.print(",");
        targetId.unparse(writer, 0, 0);
        if (timeField != null) {
            writer.print(",");
            timeField.unparse(writer, 0, 0);
        }
        writer.print(",");
        type.unparse(writer, 0, 0);
        writer.print(",");
        direct.unparse(writer, 0, 0);
        writer.print(")");
    }

    public SqlIdentifier getSourceId() {
        return sourceId;
    }

    public SqlIdentifier getTargetId() {
        return targetId;
    }

    public SqlNode getType() {
        return type;
    }

    public SqlNode getDirect() {
        return direct;
    }

    public SqlNode getTimeField() {
        return timeField;
    }
}
