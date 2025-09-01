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

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class AbstractSqlGraphElementConstruct extends SqlBasicCall {

    private final SqlIdentifier[] keyNodes;

    public AbstractSqlGraphElementConstruct(SqlOperator operator, SqlNode[] operands,
                                            SqlParserPos pos) {
        super(operator, getValueNodes(operands), pos);
        this.keyNodes = getKeyNodes(operands);
    }

    public SqlIdentifier[] getKeyNodes() {
        return keyNodes;
    }

    public SqlNode[] getValueNodes() {
        return super.operands;
    }

    protected static SqlIdentifier[] getKeyNodes(SqlNode[] operands) {
        SqlIdentifier[] keyNodes = new SqlIdentifier[operands.length / 2];
        for (int i = 0; i < operands.length; i += 2) {
            keyNodes[i / 2] = (SqlIdentifier) operands[i];
        }
        return keyNodes;
    }

    protected static SqlNode[] getValueNodes(SqlNode[] operands) {
        Preconditions.checkArgument(operands.length % 2 == 0,
            "Illegal operand count: " + operands.length);
        SqlNode[] valueNodes = new SqlNode[operands.length / 2];
        for (int i = 1; i < operands.length; i += 2) {
            valueNodes[(i - 1) / 2] = operands[i];
        }
        return valueNodes;
    }

    protected static SqlNode[] getOperands(SqlNode[] keyNodes, SqlNode[] valueNodes) {
        assert keyNodes.length == valueNodes.length;
        SqlNode[] nodes = new SqlNode[keyNodes.length + valueNodes.length];
        for (int i = 0; i < nodes.length; i++) {
            if (i % 2 == 0) {
                nodes[i] = keyNodes[i / 2];
            } else {
                nodes[i] = valueNodes[i / 2];
            }
        }
        return nodes;
    }
}
