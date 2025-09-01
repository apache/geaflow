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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;

public class StepJoinFunctionImpl implements StepJoinFunction {

    private final JoinRelType joinType;

    private final IType<?>[] leftTypes;

    private final IType<?>[] rightTypes;

    private final Expression condition;

    public StepJoinFunctionImpl(JoinRelType joinType, IType<?>[] leftTypes, IType<?>[] rightTypes) {
        this(joinType, leftTypes, rightTypes, null);
    }

    public StepJoinFunctionImpl(JoinRelType joinType, IType<?>[] leftTypes,
                                IType<?>[] rightTypes, Expression condition) {
        this.joinType = joinType;
        this.leftTypes = Objects.requireNonNull(leftTypes);
        this.rightTypes = Objects.requireNonNull(rightTypes);
        this.condition = condition;
    }

    @Override
    public Path join(Path left, Path right) {
        switch (joinType) {
            case INNER:
                if (left == null || right == null) {
                    return null;
                }
                Path innerJoinPath = joinPath(left, right);
                if (condition == null || Boolean.valueOf(true).equals(condition.evaluate(innerJoinPath))) {
                    return innerJoinPath;
                } else {
                    return null;
                }
            case LEFT:
                if (left == null) {
                    return null;
                }
                if (right == null) {
                    right = new DefaultPath(new Row[rightTypes.length]);
                }
                Path leftJoinPath = joinPath(left, right);
                if (condition == null || Boolean.valueOf(true).equals(condition.evaluate(leftJoinPath))) {
                    return leftJoinPath;
                } else {
                    return null;
                }
            case RIGHT:
                if (right == null) {
                    return null;
                }
                if (left == null) {
                    left = new DefaultPath(new Row[leftTypes.length]);
                }
                Path rightJoinPath = joinPath(left, right);
                if (condition == null || Boolean.valueOf(true).equals(condition.evaluate(rightJoinPath))) {
                    return rightJoinPath;
                } else {
                    return null;
                }
            default:
                throw new GeaFlowDSLException("JoinType: " + joinType + " is not support");
        }
    }

    @Override
    public JoinRelType getJoinType() {
        return joinType;
    }

    private Path joinPath(Path left, Path right) {
        Row[] joinPaths = new Row[leftTypes.length + rightTypes.length];
        int i;
        for (i = 0; i < leftTypes.length; i++) {
            joinPaths[i] = left.getField(i, leftTypes[i]);
        }
        for (; i < leftTypes.length + rightTypes.length; i++) {
            int rightIndex = i - leftTypes.length;
            joinPaths[i] = right.getField(rightIndex, rightTypes[rightIndex]);
        }
        return new DefaultPath(joinPaths);
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
        return new StepJoinFunctionImpl(joinType, leftTypes, rightTypes, condition);
    }
}
