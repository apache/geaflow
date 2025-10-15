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

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;

public class IsNotNullExpression extends AbstractNonLeafExpression {

    public IsNotNullExpression(Expression input) {
        super(Collections.singletonList(input), Types.BOOLEAN);
    }

    @Override
    public Object evaluate(Row row) {
        return inputs.get(0).evaluate(row) != null;
    }

    @Override
    public String showExpression() {
        return inputs.get(0).showExpression() + " is not null";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 1;
        return new IsNotNullExpression(inputs.get(0));
    }
}
