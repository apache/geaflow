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

package org.apache.geaflow.dsl.runtime.expression.logic;

import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;

/**
 * Expression for IS TYPED predicate.
 * Checks if the value of an expression matches the target type.
 * Returns true if the value's runtime type matches the target type, false otherwise.
 * NULL values return false.
 */
public class IsTypedExpression extends AbstractNonLeafExpression {

    private final IType<?> targetType;

    public IsTypedExpression(Expression input, IType<?> targetType) {
        super(Arrays.asList(input), Types.BOOLEAN);
        this.targetType = targetType;
    }

    @Override
    public Object evaluate(Row row) {
        Object value = inputs.get(0).evaluate(row);
        
        // NULL values are not considered typed
        if (value == null) {
            return false;
        }
        
        // Check if the value's type matches the target type
        return isCompatibleType(value, targetType);
    }

    /**
     * Check if a value is compatible with the target type.
     * This method performs type checking for both primitive and complex types.
     */
    private boolean isCompatibleType(Object value, IType<?> targetType) {
        if (value == null) {
            return false;
        }
        
        String targetTypeName = targetType.getName();
        Class<?> valueClass = value.getClass();
        Class<?> targetClass = targetType.getTypeClass();
        
        // Direct type match
        if (targetClass.isInstance(value)) {
            return true;
        }
        
        // Handle numeric type compatibility
        // e.g., a Long value might match both LONG and INTEGER with truncation
        if (isNumericType(targetTypeName)) {
            return isNumericCompatible(value, targetTypeName);
        }
        
        return false;
    }

    /**
     * Check if value is numeric-compatible with the target type.
     * Handles numeric type hierarchy: BYTE < SHORT < INTEGER < LONG < FLOAT < DOUBLE
     */
    private boolean isNumericCompatible(Object value, String targetTypeName) {
        if (!(value instanceof Number)) {
            return false;
        }
        
        Number numValue = (Number) value;
        
        switch (targetTypeName) {
            case "BYTE":
                return value instanceof Byte;
            case "SHORT":
                return value instanceof Short || value instanceof Byte;
            case "INTEGER":
                return value instanceof Integer || value instanceof Short || value instanceof Byte;
            case "LONG":
                return value instanceof Long || value instanceof Integer || 
                       value instanceof Short || value instanceof Byte;
            case "FLOAT":
                return value instanceof Float || value instanceof Number;
            case "DOUBLE":
                return value instanceof Double || value instanceof Number;
            default:
                return false;
        }
    }

    /**
     * Check if the target type is a numeric type.
     */
    private boolean isNumericType(String typeName) {
        return typeName.equals("BYTE") || typeName.equals("SHORT") || 
               typeName.equals("INTEGER") || typeName.equals("LONG") ||
               typeName.equals("FLOAT") || typeName.equals("DOUBLE");
    }

    @Override
    public String showExpression() {
        return inputs.get(0).showExpression() + " IS TYPED " + targetType.getName();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 1;
        return new IsTypedExpression(inputs.get(0), targetType);
    }

    public IType<?> getTargetType() {
        return targetType;
    }
}

