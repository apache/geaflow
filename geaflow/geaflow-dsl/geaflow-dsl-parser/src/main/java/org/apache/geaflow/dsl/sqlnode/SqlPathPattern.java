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
import java.util.Objects;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlPathPatternOperator;

public class SqlPathPattern extends SqlCall {

    private SqlNodeList pathNodes;

    private SqlIdentifier pathAlias;


    public SqlPathPattern(SqlParserPos pos, SqlNodeList pathNodes, SqlIdentifier pathAlias) {
        super(pos);
        this.pathNodes = Objects.requireNonNull(pathNodes);
        this.pathAlias = pathAlias;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlPathPatternOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(pathNodes, pathAlias);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.pathNodes = (SqlNodeList) operand;
                break;
            case 1:
                this.pathAlias = (SqlIdentifier) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (pathAlias != null) {
            pathAlias.unparse(writer, 0, 0);
            writer.print("=");
        }

        for (SqlNode node : pathNodes) {
            node.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNodeList getPathNodes() {
        return pathNodes;
    }

    public String getPathAliasName() {
        if (pathAlias != null) {
            return pathAlias.getSimple();
        }
        return null;
    }

    public void setPathAlias(SqlIdentifier pathAlias) {
        this.pathAlias = pathAlias;
    }

    public SqlMatchNode getFirst() {
        return (SqlMatchNode) pathNodes.get(0);
    }

    public SqlMatchNode getLast() {
        return (SqlMatchNode) pathNodes.get(pathNodes.size() - 1);
    }
}
