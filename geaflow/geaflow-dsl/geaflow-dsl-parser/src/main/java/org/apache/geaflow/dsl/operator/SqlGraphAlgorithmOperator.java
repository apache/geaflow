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

package org.apache.geaflow.dsl.operator;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public class SqlGraphAlgorithmOperator extends SqlOperator {

    public static final SqlGraphAlgorithmOperator INSTANCE = new SqlGraphAlgorithmOperator();

    protected SqlGraphAlgorithmOperator() {
        super("Graph Algorithm", SqlKind.GQL_ALGORITHM, 2, false,
            ReturnTypes.ARG1, new AlgorithmOperandTypeInfer(), null);
    }

    @Override
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        return true;
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    private static class AlgorithmOperandTypeInfer implements SqlOperandTypeInference {

        @Override
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
            List<RelDataType> callOperandTypes = callBinding.collectOperandTypes();
            for (int i = 0; i < callOperandTypes.size(); i++) {
                operandTypes[i] = callOperandTypes.get(i);
            }
        }
    }
}
