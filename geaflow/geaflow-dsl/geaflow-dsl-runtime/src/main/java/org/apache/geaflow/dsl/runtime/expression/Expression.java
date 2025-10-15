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

package org.apache.geaflow.dsl.runtime.expression;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.function.FunctionContext;

public interface Expression extends Serializable {

    /**
     * Open method for expression.
     */
    default void open(FunctionContext context) {
    }

    /**
     * Evaluate the value of this expression with the given input.
     */
    Object evaluate(Row row);

    /**
     * Show the expression string.
     */
    String showExpression();

    /**
     * Get the output type of this expression.
     */
    IType<?> getOutputType();

    List<Expression> getInputs();

    Expression copy(List<Expression> inputs);

    List<Integer> getRefPathFieldIndices();

    Expression replace(Function<Expression, Expression> replaceFn);

    List<Expression> collect(Predicate<Expression> condition);

    List<Expression> splitByAnd();
}
