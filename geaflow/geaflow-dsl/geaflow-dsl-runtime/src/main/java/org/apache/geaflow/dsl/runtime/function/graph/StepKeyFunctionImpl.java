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
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.ObjectRowKey;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;

public class StepKeyFunctionImpl implements StepKeyFunction {

    private final int[] keyIndices;

    private final IType<?>[] keyTypes;

    public StepKeyFunctionImpl(int[] keyIndices, IType<?>[] keyTypes) {
        this.keyIndices = keyIndices;
        this.keyTypes = keyTypes;
        assert keyIndices.length == keyTypes.length;
    }

    @Override
    public RowKey getKey(Row row) {
        Object[] keys = new Object[keyIndices.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = row.getField(keyIndices[i], keyTypes[i]);
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
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.isEmpty();
        return new StepKeyFunctionImpl(keyIndices, keyTypes);
    }
}
