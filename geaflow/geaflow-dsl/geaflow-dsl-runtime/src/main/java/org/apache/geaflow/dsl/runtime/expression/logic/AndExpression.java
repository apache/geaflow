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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;

public class AndExpression extends AbstractNonLeafExpression {

    public AndExpression(List<Expression> inputs) {
        super(inputs, Types.BOOLEAN);
    }

    @Override
    public Object evaluate(Row row) {
        boolean hasNull = false;

        for (Expression input : inputs) {
            Boolean b = (Boolean) input.evaluate(row);
            if (b != null && !b) {
                return false;
            }
            if (b == null) {
                hasNull = true;
            }
        }
        if (hasNull) {
            return null;
        } else {
            return true;
        }
    }

    @Override
    public String showExpression() {
        return "And(" + inputs.stream().map(Expression::showExpression)
            .collect(Collectors.joining(",")) + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new AndExpression(inputs);
    }
}
