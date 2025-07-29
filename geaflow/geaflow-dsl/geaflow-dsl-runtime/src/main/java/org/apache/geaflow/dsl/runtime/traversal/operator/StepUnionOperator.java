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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import org.apache.geaflow.dsl.runtime.function.graph.StepFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepUnionOperator.StepUnionFunction;
import org.apache.geaflow.dsl.runtime.util.SchemaUtil;

public class StepUnionOperator extends AbstractStepOperator<StepUnionFunction, StepRecordWithPath, StepRecordWithPath> {

    private final Map<Long, PathType> inputSchemas = new HashMap<>();

    public StepUnionOperator(long id) {
        super(id, StepUnionFunction.INSTANCE);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        List<Long> inputIds = context.getTopology().getInputIds(id);
        for (long inputId : inputIds) {
            PathType pathType = context.getTopology().getOperator(inputId).getOutputPathSchema();
            this.inputSchemas.put(inputId, pathType);
        }
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        final long inputId = context.getInputOperatorId();
        PathType pathType = inputSchemas.get(inputId);
        StepRecordWithPath unionPath = record.mapPath(path ->
            SchemaUtil.alignToPathSchema(path, pathType, outputPathSchema), null);
        collect(unionPath);
    }

    @Override
    protected PathType concatInputPathType() {
        return this.getOutputPathSchema();
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepUnionOperator(id);
    }

    public static class StepUnionFunction implements StepFunction {

        public static final StepUnionFunction INSTANCE = new StepUnionFunction();

        private StepUnionFunction() {

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
            return new StepUnionFunction();
        }
    }
}
