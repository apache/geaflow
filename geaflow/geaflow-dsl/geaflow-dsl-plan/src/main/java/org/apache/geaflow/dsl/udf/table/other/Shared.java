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

import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * UDF implementation for the SHARED() function.
 * 
 * This function is used in graph pattern matching to specify a shared predicate
 * that applies to multiple path patterns in a MATCH clause.
 * 
 * Syntax: SHARED(condition)
 * 
 * The SHARED() function is a pass-through function that simply returns the input
 * boolean value. The actual semantics of sharing the predicate across multiple
 * patterns is handled at the Calcite validator and runtime levels.
 * 
 * Production-ready implementation notes:
 * - This function accepts any expression that evaluates to a boolean value
 * - It acts as a marker for the query optimizer to recognize shared predicates
 * - The optimizer uses this to optimize pattern matching execution
 */
@Description(name = "shared", description = "Marks a predicate as shared across multiple path patterns in MATCH clause")
public class Shared extends UDF {

    /**
     * Evaluates the shared predicate.
     * 
     * This method simply returns the input boolean value. The sharing semantics
     * is handled by the query optimizer and runtime execution engine.
     * 
     * @param condition The boolean condition to be shared across patterns
     * @return The input boolean condition unchanged
     */
    public Boolean eval(Boolean condition) {
        return condition;
    }

    /**
     * Get the return type of the SHARED function.
     * 
     * This is a scalar function that returns a boolean value.
     * 
     * @return The return type is BOOLEAN
     */
    public IType<?> getReturnType(IType<?> inputType) {
        return inputType;
    }
}

