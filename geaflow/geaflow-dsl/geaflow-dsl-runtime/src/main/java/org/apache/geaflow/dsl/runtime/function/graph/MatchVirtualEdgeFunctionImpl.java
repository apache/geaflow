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
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;

public class MatchVirtualEdgeFunctionImpl implements MatchVirtualEdgeFunction {

    private final Expression targetId;

    public MatchVirtualEdgeFunctionImpl(Expression targetId) {
        this.targetId = targetId;
    }

    @Override
    public List<Object> computeTargetId(Path path) {
        Object targetId = this.targetId.evaluate(path);
        return Collections.singletonList(targetId);
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.singletonList(targetId);
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.size() == 1;
        return new MatchVirtualEdgeFunctionImpl(expressions.get(0));
    }
}
