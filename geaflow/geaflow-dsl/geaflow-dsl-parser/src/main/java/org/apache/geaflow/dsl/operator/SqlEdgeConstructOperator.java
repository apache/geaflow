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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlEdgeConstruct;

public class SqlEdgeConstructOperator extends SqlMultisetValueConstructor {

    private final SqlIdentifier[] keyNodes;

    public SqlEdgeConstructOperator(SqlIdentifier[] keyNodes) {
        super("EDGE", SqlKind.EDGE_VALUE_CONSTRUCTOR);
        this.keyNodes = keyNodes;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        List<RelDataType> valuesType = opBinding.collectOperandTypes();
        if (keyNodes.length != valuesType.size()) {
            throw new GeaFlowDSLException(String.format("Key size: %s is not equal to the value size: %s at %s",
                keyNodes.length, valuesType.size(), keyNodes[0].getParserPosition()));
        }
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < keyNodes.length; i++) {
            String name = keyNodes[i].getSimple();
            RelDataTypeField field = new RelDataTypeFieldImpl(name, i, valuesType.get(i));
            fields.add(field);
        }
        return EdgeRecordType.createEdgeType(fields, opBinding.getTypeFactory());
    }

    @Override
    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            RelDataType nodeType = validator.deriveType(scope, operand);
            assert nodeType != null;
        }
        RelDataType type = call.getOperator().validateOperands(validator, scope, call);
        SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
        if (type.getSqlTypeName() != SqlTypeName.EDGE) {
            throw new GeaFlowDSLException("Edge construct must return edge type, current is: "
                + type + " at " + call.getParserPosition());
        }
        return type;
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
        pos = pos.plusAll(Arrays.asList(operands));
        return new SqlEdgeConstruct(keyNodes, operands, pos);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        call.unparse(writer, leftPrec, rightPrec);
    }
}
