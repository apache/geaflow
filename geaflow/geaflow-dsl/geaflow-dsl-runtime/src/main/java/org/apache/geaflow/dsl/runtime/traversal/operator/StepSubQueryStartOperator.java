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

package org.apache.geaflow.dsl.runtime.traversal.operator;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import org.apache.geaflow.dsl.runtime.function.graph.StepFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.CallRequestId;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;

public class StepSubQueryStartOperator extends AbstractStepOperator<StepFunction, VertexRecord, VertexRecord> {

    private final String queryName;

    public StepSubQueryStartOperator(long id, String queryName) {
        super(id, SubQueryStartFunction.INSTANCE);
        this.queryName = Objects.requireNonNull(queryName);
    }

    @Override
    protected void processRecord(VertexRecord record) {
        Object requestId = context.getRequestId();
        if (requestId instanceof CallRequestId) {
            context.stashCallRequestId((CallRequestId) requestId);
        }
        collect(record);
    }

    @Override
    protected boolean hasReceivedAllEod(List<EndOfData> receiveEods) {
        return receiveEods.size() == numTasks;
    }

    @Override
    public StepOperator<VertexRecord, VertexRecord> copyInternal() {
        return new StepSubQueryStartOperator(id, queryName);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        str.append("(name=").append(queryName).append(")");
        return str.toString();
    }

    public String getQueryName() {
        return queryName;
    }

    private static class SubQueryStartFunction implements StepFunction {

        public static final SubQueryStartFunction INSTANCE = new SubQueryStartFunction();

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
            return new SubQueryStartFunction();
        }
    }
}
