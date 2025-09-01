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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.ObjectSingleValue;
import org.apache.geaflow.dsl.runtime.traversal.data.SingleValue;

public class StepAggFunctionImpl implements StepAggregateFunction {

    private final UDAF<Object, Object, Object> udaf;

    private final IType<?> inputType;

    public StepAggFunctionImpl(UDAF<Object, Object, Object> udaf, IType<?> inputType) {
        this.udaf = Objects.requireNonNull(udaf);
        this.inputType = Objects.requireNonNull(inputType);
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {
        udaf.open(FunctionContext.of(context.getConfig()));
    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        return new StepAggFunctionImpl(udaf, inputType);
    }

    @Override
    public Object createAccumulator() {
        return udaf.createAccumulator();
    }

    @Override
    public void add(Row row, Object accumulator) {
        Object input = row.getField(0, inputType);
        udaf.accumulate(accumulator, input);
    }

    @Override
    public void merge(Object acc, Object otherAcc) {
        udaf.merge(acc, Lists.newArrayList(otherAcc));
    }

    @Override
    public SingleValue getValue(Object accumulator) {
        Object value = udaf.getValue(accumulator);
        return ObjectSingleValue.of(value);
    }
}
