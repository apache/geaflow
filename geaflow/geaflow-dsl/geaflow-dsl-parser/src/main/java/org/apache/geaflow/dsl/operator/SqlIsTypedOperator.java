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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * SQL Operator for IS TYPED predicate (ISO-GQL standard).
 * Checks if a value is of a specified type.
 * Example: n.age IS TYPED INTEGER
 */
public class SqlIsTypedOperator extends SqlOperator {

    public static final SqlIsTypedOperator INSTANCE = new SqlIsTypedOperator();

    protected SqlIsTypedOperator() {
        super("IS TYPED", SqlKind.OTHER, 2, true,
            ReturnTypes.BOOLEAN, null, null);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        // Validate operand count: must be exactly 2
        final int operandCount = callBinding.getOperandCount();
        if (operandCount != 2) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        // First operand: the expression to check (can be any type)
        // We accept any expression type - validation happens at runtime
        final RelDataType type0 = callBinding.getOperandType(0);
        if (type0 == null) {
            // Null type is acceptable - will be checked at runtime
            return true;
        }

        // Second operand: must be a type identifier (SqlIdentifier)
        final SqlNode operand1 = callBinding.operand(1);
        if (!(operand1 instanceof SqlIdentifier)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        // Validate that the type identifier is a valid SQL type name
        final SqlIdentifier typeId = (SqlIdentifier) operand1;
        final String typeName = typeId.getSimple().toUpperCase();
        
        // Check if it's a standard SQL type or a valid custom type
        try {
            SqlTypeName.valueOf(typeName);
            return true;
        } catch (IllegalArgumentException e) {
            // Also allow GeaFlow custom types: VERTEX, EDGE, PATH, BINARY_STRING
            if (isValidCustomType(typeName)) {
                return true;
            }
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
    }

    /**
     * Check if the type name is a valid GeaFlow custom type.
     * 
     * @param typeName the type name in uppercase
     * @return true if it's a valid custom type, false otherwise
     */
    private boolean isValidCustomType(String typeName) {
        // GeaFlow graph-specific custom types
        return "VERTEX".equals(typeName) 
            || "EDGE".equals(typeName) 
            || "PATH".equals(typeName)
            || "BINARY_STRING".equals(typeName);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.POSTFIX;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        // Unparse as: expression IS TYPED typename
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("IS TYPED");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
}

