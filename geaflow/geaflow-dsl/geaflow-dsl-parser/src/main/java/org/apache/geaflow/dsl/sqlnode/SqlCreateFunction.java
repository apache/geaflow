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

import com.google.common.base.Objects;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.util.StringLiteralUtil;

/**
 * Parse tree node that represents a CREATE Function statement.
 */
public class SqlCreateFunction extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

    private SqlNode functionName;
    private SqlNode className;
    private SqlNode usingPath;

    public SqlCreateFunction(SqlParserPos pos,
                             boolean ifNotExists,
                             SqlNode functionName,
                             SqlNode className,
                             SqlNode usingPath) {
        super(OPERATOR, pos, false, ifNotExists);
        this.functionName = functionName;
        this.className = className;
        this.usingPath = usingPath;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getFunctionName(), getClassNameNode(), usingPath);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.functionName = operand;
                break;
            case 1:
                this.className = operand;
                break;
            case 2:
                this.usingPath = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("FUNCTION");
        if (super.ifNotExists) {
            writer.keyword("IF");
            writer.keyword("NOT");
            writer.keyword("EXISTS");
        }
        functionName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        className.unparse(writer, 0, 0);
        if (usingPath != null) {
            writer.keyword("USING");
            usingPath.unparse(writer, 0, 0);
        }
    }

    public void validate() {

    }

    public SqlNode getFunctionName() {
        return functionName;
    }


    public String getClassName() {
        return StringLiteralUtil.unescapeSQLString(className.toString());
    }

    public String getUsingPath() {
        if (usingPath == null) {
            return null;
        }
        return StringLiteralUtil.unescapeSQLString(usingPath.toString());
    }

    public SqlNode getClassNameNode() {
        return className;
    }

    public void setClassName(SqlNode className) {
        this.className = className;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlCreateFunction that = (SqlCreateFunction) o;
        return Objects.equal(functionName, that.functionName)
            && Objects.equal(className, that.className)
            && Objects.equal(usingPath, that.usingPath);
    }

    @Override
    public int hashCode() {
        return Objects
            .hashCode(functionName, className, usingPath);
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }
}
