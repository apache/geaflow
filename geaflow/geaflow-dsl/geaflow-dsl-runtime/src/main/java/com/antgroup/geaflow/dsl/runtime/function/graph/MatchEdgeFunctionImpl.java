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

package com.antgroup.geaflow.dsl.runtime.function.graph;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EmptyFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class MatchEdgeFunctionImpl implements MatchEdgeFunction {

    private final EdgeDirection direction;

    private final Set<BinaryString> edgeTypes;

    private final String label;

    private IFilter edgesFilter;

    private final boolean isOptionalMatchEdge;

    public MatchEdgeFunctionImpl(EdgeDirection direction, Set<BinaryString> edgeTypes,
                                 boolean isOptionalMatchEdge, String label, IFilter<?> edgeFilter) {
        this.direction = direction;
        this.edgeTypes = edgeTypes;
        this.label = label;
        this.edgesFilter = edgeFilter;
        this.isOptionalMatchEdge = isOptionalMatchEdge;
    }

    public MatchEdgeFunctionImpl(EdgeDirection direction, Set<BinaryString> edgeTypes, String label,
                                 IFilter<?> edgeFilter) {
        this(direction, edgeTypes, false, label, edgeFilter);
    }

    public MatchEdgeFunctionImpl(EdgeDirection direction, Set<BinaryString> edgeTypes, String label,
                                 IFilter ... pushDownFilter) {
        this(direction, edgeTypes, false, label, pushDownFilter);
    }

    public MatchEdgeFunctionImpl(EdgeDirection direction, Set<BinaryString> edgeTypes,
                                 boolean isOptionalMatchEdge, String label,
                                  IFilter ... pushDownFilter) {
        this.direction = direction;
        this.edgeTypes = Objects.requireNonNull(edgeTypes);
        this.label = label;
        IFilter directionFilter;
        switch (direction) {
            case OUT:
                directionFilter = OutEdgeFilter.getInstance();
                break;
            case IN:
                directionFilter = InEdgeFilter.getInstance();
                break;
            case BOTH:
                directionFilter = EmptyFilter.getInstance();
                break;
            default:
                throw new IllegalArgumentException("Illegal edge direction: " + direction);
        }
        this.edgesFilter = directionFilter;
        if (!edgeTypes.isEmpty()) {
            this.edgesFilter.and(new EdgeLabelFilter(edgeTypes.stream().map(BinaryString::toString)
                .collect(Collectors.toSet())));
        }
        for (IFilter andFilter : pushDownFilter) {
            this.edgesFilter = this.edgesFilter == null ? andFilter :
                               this.edgesFilter.and(andFilter);
        }
        this.isOptionalMatchEdge = isOptionalMatchEdge;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public EdgeDirection getDirection() {
        return direction;
    }

    @Override
    public Set<BinaryString> getEdgeTypes() {
        return edgeTypes;
    }

    public boolean isOptionalMatchEdge() {
        return isOptionalMatchEdge;
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public IFilter getEdgesFilter() {
        return edgesFilter;
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.isEmpty();
        return new MatchEdgeFunctionImpl(direction, edgeTypes, label, edgesFilter);
    }
}
