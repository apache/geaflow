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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlGraphAlgorithmOperator;

/**
 * Parse tree node that represents a CREATE TABLE statement.
 */
public class SqlGraphAlgorithmCall extends SqlBasicCall {

    private static final SqlOperator OPERATOR = SqlGraphAlgorithmOperator.INSTANCE;

    private SqlNode from;
    private SqlIdentifier algorithm;
    private SqlNodeList parameters;
    private SqlNodeList yields;

    /**
     * Creates a SqlCreateTable.
     */
    public SqlGraphAlgorithmCall(SqlParserPos pos,
                                 SqlNode from,
                                 SqlIdentifier algorithm,
                                 SqlNodeList parameters,
                                 SqlNodeList yields) {
        super(OPERATOR, parameters == null ? null : parameters.toArray(), pos);
        this.from = from;
        this.algorithm = algorithm;
        this.parameters = parameters;
        this.yields = yields;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getFrom(), getAlgorithm(), getParameters(), getYields());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.from = operand;
                break;
            case 1:
                this.algorithm = (SqlIdentifier) operand;
                break;
            case 2:
                this.parameters = (SqlNodeList) operand;
                break;
            case 3:
                this.yields = (SqlNodeList) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CALL");
        algorithm.unparse(writer, leftPrec, rightPrec);
        if (parameters != null && parameters.size() >= 0) {
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.newlineAndIndent();
            writer.print("  ");
            if (parameters.size() > 0) {
                for (int i = 0; i < parameters.size(); i++) {
                    if (i > 0) {
                        writer.print(",");
                        writer.newlineAndIndent();
                        writer.print("  ");
                    }
                    parameters.get(i).unparse(writer, leftPrec, rightPrec);
                }
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        } else {
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.endList(frame);
        }
        writer.keyword("YIELD");
        writer.print(" ");
        if (yields != null && yields.size() > 0) {
            final SqlWriter.Frame with =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.newlineAndIndent();
            for (int i = 0; i < yields.size(); i++) {
                if (i > 0) {
                    writer.print(",");
                    writer.newlineAndIndent();
                }
                yields.get(i).unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(with);
        } else {
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.endList(frame);
        }
    }

    /**
     * Sql syntax validation.
     */
    public void validate() throws ValidationException {
        Map<String, Boolean> columnNameMap = new HashMap<>();
        if (yields != null) {
            for (SqlNode yield : yields) {
                String yieldName = ((SqlIdentifier) yield).getSimple();
                if (columnNameMap.get(yieldName) == null) {
                    columnNameMap.put(yieldName, true);
                } else {
                    throw new ValidationException(
                        "duplicate yield name " + "[" + yieldName + "], at " + yield.getParserPosition());
                }
            }
        }
    }

    public SqlIdentifier getAlgorithm() {
        return algorithm;
    }

    public SqlNodeList getParameters() {
        return parameters;
    }

    public SqlNodeList getYields() {
        return yields;
    }

    public SqlNode getFrom() {
        return from;
    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }
}
