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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.geaflow.dsl.operator.SqlEdgeConstructOperator;

public class SqlEdgeConstruct extends AbstractSqlGraphElementConstruct {

    public SqlEdgeConstruct(SqlNode[] operands, SqlParserPos pos) {
        super(new SqlEdgeConstructOperator(getKeyNodes(operands)), operands, pos);
    }

    public SqlEdgeConstruct(SqlIdentifier[] keyNodes, SqlNode[] valueNodes, SqlParserPos pos) {
        this(getOperands(keyNodes, valueNodes), pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("Edge");
        writer.print("{\n");
        for (int i = 0; i < getKeyNodes().length; i++) {
            SqlNode key = getKeyNodes()[i];
            SqlNode value = getValueNodes()[i];
            key.unparse(writer, 0, 0);
            writer.print("=");
            value.unparse(writer, 0, 0);
            writer.print("\n");
        }
        writer.print("}");
    }
}
