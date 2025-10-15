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

package org.apache.geaflow.dsl.runtime.function.graph;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;

public class StepBoolFunctionImpl implements StepBoolFunction {

    private final Expression condition;

    public StepBoolFunctionImpl(Expression condition) {
        this.condition = Objects.requireNonNull(condition);
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {
        StepFunction.openExpression(condition, context);
    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public boolean filter(Row record) {
        Boolean accept = (Boolean) condition.evaluate(record);
        return accept != null && accept;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.singletonList(condition);
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.size() == 1;
        return new StepBoolFunctionImpl(expressions.get(0));
    }

}
