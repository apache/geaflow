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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.geaflow.dsl.operator.SqlMatchNodeOperator;

public class SqlMatchNode extends SqlCall {

    private SqlIdentifier name;

    private SqlNodeList labels;

    private SqlNode where;

    private SqlNode combineWhere;

    private SqlNodeList propertySpecification;

    public SqlMatchNode(SqlParserPos pos, SqlIdentifier name, SqlNodeList labels,
                        SqlNodeList propertySpecification, SqlNode where) {
        super(pos);
        this.name = name;
        this.labels = labels;
        this.propertySpecification = propertySpecification;
        this.where = where;
        this.combineWhere = getInnerWhere(propertySpecification, where);
    }

    @Override
    public SqlOperator getOperator() {
        return SqlMatchNodeOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_NODE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, labels, propertySpecification, where);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.labels = (SqlNodeList) operand;
                break;
            case 2:
                this.propertySpecification = (SqlNodeList) operand;
                break;
            case 3:
                this.where = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("(");
        unparseNode(writer);
        writer.print(")");
    }

    protected void unparseNode(SqlWriter writer) {
        if (name != null) {
            name.unparse(writer, 0, 0);
        }
        if (labels != null && labels.size() > 0) {
            writer.print(":");
            for (int i = 0; i < labels.size(); i++) {
                SqlNode label = labels.get(i);
                if (i > 0) {
                    writer.print("|");
                }
                label.unparse(writer, 0, 0);
            }
        }

        if (where != null) {
            writer.keyword("where");
            where.unparse(writer, 0, 0);
        }
        if (propertySpecification != null && propertySpecification.size() > 0) {
            writer.keyword("{");
            int idx = 0;
            for (SqlNode node : propertySpecification.getList()) {
                if (idx % 2 != 0) {
                    writer.keyword(":");
                } else if (idx > 0) {
                    writer.keyword(",");
                }
                node.unparse(writer, 0, 0);
                idx++;
            }
            writer.keyword("}");
        }
    }

    public SqlIdentifier getNameId() {
        return name;
    }

    public SqlIdentifier setName(SqlIdentifier name) {
        this.name = name;
        return this.name;
    }

    public SqlNodeList getLabels() {
        return labels;
    }

    public List<String> getLabelNames() {
        if (labels == null) {
            return Collections.emptyList();
        }
        List<String> labelNames = new ArrayList<>(labels.size());
        for (SqlNode labelNode : labels) {
            SqlIdentifier label = (SqlIdentifier) labelNode;
            labelNames.add(label.getSimple());
        }
        return labelNames;
    }

    public String getName() {
        if (name != null) {
            return name.getSimple();
        } else {
            return null;
        }
    }

    public SqlNode getWhere() {
        return combineWhere;
    }

    public static SqlNode getInnerWhere(SqlNodeList propertySpecification, SqlNode where) {
        //If where is null but property specification is not null,
        // construct where SqlNode use the property specification.
        if (propertySpecification != null && propertySpecification.size() >= 2) {
            List<SqlNode> propertyEqualsConditions = new ArrayList<>();

            for (int idx = 0; idx < propertySpecification.size(); idx += 2) {
                SqlNode left = propertySpecification.get(idx);
                SqlNode right = propertySpecification.get(idx + 1);
                SqlNode equalsCondition = makeEqualsSqlNode(left, right);
                propertyEqualsConditions.add(equalsCondition);
            }
            if (where != null) {
                propertyEqualsConditions.add(where);
            }
            return makeAndSqlNode(propertyEqualsConditions);
        }
        return where;
    }

    private static SqlNode makeEqualsSqlNode(SqlNode leftNode, SqlNode rightNode) {
        return new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[]{leftNode, rightNode},
            leftNode.getParserPosition());
    }

    private static SqlNode makeAndSqlNode(List<SqlNode> conditions) {
        if (conditions.size() == 1) {
            return conditions.get(0);  // 只有一个条件时直接返回
        } else {
            return new SqlBasicCall(SqlStdOperatorTable.AND, conditions.toArray(new SqlNode[0]),
                conditions.get(0).getParserPosition());
        }
    }

    public void setWhere(SqlNode where) {
        this.combineWhere = where;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }
}
