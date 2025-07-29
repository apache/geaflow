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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.ObjectRowKey;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;

public class StepKeyExpressionFunctionImpl implements StepKeyFunction {

    private final Expression[] keyExpressions;

    private final IType<?>[] keyTypes;

    public StepKeyExpressionFunctionImpl(Expression[] keyIndices, IType<?>[] keyTypes) {
        this.keyExpressions = keyIndices;
        this.keyTypes = keyTypes;
        assert keyIndices.length == keyTypes.length;
    }

    @Override
    public RowKey getKey(Row row) {
        Object[] keys = new Object[keyExpressions.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyExpressions[i].evaluate(row);
        }
        return ObjectRowKey.of(keys);
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public List<Expression> getExpressions() {
        return Arrays.stream(keyExpressions).collect(Collectors.toList());
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        return new StepKeyExpressionFunctionImpl(keyExpressions, keyTypes);
    }
}
