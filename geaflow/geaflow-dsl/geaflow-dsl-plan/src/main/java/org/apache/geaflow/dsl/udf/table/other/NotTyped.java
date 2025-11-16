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

package org.apache.geaflow.dsl.udf.table.other;

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * ISO-GQL NOT_TYPED function - checks if a value is NOT of a specified type.
 * 
 * Syntax: NOT_TYPED(value, typename)
 * 
 * Returns true if the value's runtime type does NOT match the specified typename.
 * Returns true for NULL values (NULL is not of any specific type).
 * 
 * Examples:
 *   NOT_TYPED(n.age, STRING)    -> true if n.age is not a string
 *   NOT_TYPED(null, INTEGER)    -> true (NULL is not an integer)
 * 
 * Note: The actual type checking logic is implemented in IsNotTypedExpression
 * during runtime evaluation. This UDF definition allows Calcite to recognize
 * NOT_TYPED as a valid SQL function during parsing and planning phases.
 * The real implementation happens in the expression evaluation layer.
 */
@Description(name = "not_typed", description = "Checks if a value is NOT of a specified type (ISO-GQL Value Type Predicate)")
public class NotTyped extends UDF {

    /**
     * Check if the value is NOT of the specified type.
     * 
     * This method is called during query planning and validation.
     * The actual runtime implementation is in IsNotTypedExpression.evaluate().
     *
     * @param value The value to check (can be any SQL expression result)
     * @param typename The target type name (as a string identifier like "INTEGER", "STRING", etc.)
     * @return true if value is NOT of the specified type or is NULL, false otherwise
     *         - Returns true if value is NULL (NULL is not of any specific type)
     *         - Returns true if value's runtime type does NOT match typename
     *         - Returns false only if value's runtime type matches typename exactly
     */
    public Boolean eval(Object value, String typename) {
        // This is not the actual runtime implementation.
        // During query execution, this gets replaced by IsNotTypedExpression.
        // This placeholder exists so Calcite recognizes NOT_TYPED as a valid function.
        if (value == null) {
            return true;  // NULL is not of any specific type
        }
        
        return !matchesType(value, typename);
    }
    
    /**
     * Helper method to determine if a value matches the specified type name.
     * This provides a fallback implementation if the expression layer is bypassed.
     * 
     * @param value The value to check
     * @param typename The type name to match against
     * @return true if the value's class matches the type name
     */
    private Boolean matchesType(Object value, String typename) {
        if (value == null) {
            return false;
        }
        
        String typeName = typename != null ? typename.toUpperCase() : "";
        Class<?> valueClass = value.getClass();
        
        switch (typeName) {
            case "INTEGER":
            case "INT":
                return valueClass == Integer.class || valueClass == Long.class;
            case "DOUBLE":
            case "FLOAT":
                return valueClass == Double.class || valueClass == Float.class;
            case "STRING":
            case "VARCHAR":
                return valueClass == String.class;
            case "BOOLEAN":
            case "BOOL":
                return valueClass == Boolean.class;
            case "BINARY_STRING":
            case "BYTES":
                return valueClass == byte[].class;
            default:
                // For VERTEX, EDGE, PATH and other custom types,
                // rely on the expression layer for proper type checking
                return false;
        }
    }
}

