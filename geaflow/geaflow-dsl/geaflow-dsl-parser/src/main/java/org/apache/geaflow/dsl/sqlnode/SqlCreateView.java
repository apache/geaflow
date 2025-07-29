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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree node that represents a CREATE VIEW statement.
 */
public class SqlCreateView extends SqlCreate {

    public static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

    private SqlIdentifier name;
    private SqlNodeList fields;
    private SqlNode subQuery;

    public SqlCreateView(SqlParserPos pos,
                         boolean ifNotExists,
                         SqlIdentifier name,
                         SqlNodeList fields,
                         SqlNode subQuery) {
        super(OPERATOR, pos, false, ifNotExists);
        this.name = name;
        this.subQuery = subQuery;
        this.fields = fields;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getFields(), getSubQuery());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.fields = (SqlNodeList) operand;
                break;
            case 2:
                this.subQuery = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("VIEW");
        if (super.ifNotExists) {
            writer.keyword("IF");
            writer.keyword("NOT");
            writer.keyword("EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (fields.size() > 0) {
            final SqlWriter.Frame field =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.newlineAndIndent();
            writer.print("  ");
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    writer.print(",");
                    writer.newlineAndIndent();
                    writer.print("  ");
                }
                fields.get(i).unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(field);
        }
        writer.keyword("AS");
        writer.newlineAndIndent();
        subQuery.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public List<String> getFieldNames() {
        List<String> fieldNames = Lists.newArrayList();
        for (SqlNode node : fields.getList()) {
            fieldNames.add(node.toString());
        }
        return fieldNames;
    }

    public SqlNodeList getFields() {
        return fields;
    }

    public SqlNode getSubQuery() {
        return subQuery;
    }

    public String getSubQuerySql() {
        return subQuery.toString();
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }
}
