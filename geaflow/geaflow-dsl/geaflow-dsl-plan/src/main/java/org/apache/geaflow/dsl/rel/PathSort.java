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

package org.apache.geaflow.dsl.rel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.geaflow.dsl.calcite.PathRecordType;

public abstract class PathSort extends SingleRel {

    protected final List<RexNode> orderByExpressions;
    protected final RexNode limit;

    protected PathSort(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                       List<RexNode> orderByExpressions, RexNode limit,
                       PathRecordType pathType) {
        super(cluster, traits, input);
        this.rowType = Objects.requireNonNull(pathType);
        this.orderByExpressions = Objects.requireNonNull(orderByExpressions);
        this.limit = limit;
    }

    public List<RexNode> getOrderByExpressions() {
        return orderByExpressions;
    }

    public RexNode getLimit() {
        return limit;
    }

    public abstract PathSort copy(RelNode input, List<RexNode> orderByExpressions,
                                  RexNode limit, PathRecordType pathType);

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs != null && inputs.size() == 1 : "Invalid inputs size";
        return copy(sole(inputs), orderByExpressions, limit, (PathRecordType) rowType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<RexNode> newOrders = orderByExpressions.stream()
            .map(shuttle::apply).collect(Collectors.toList());
        RexNode newLimit = null;
        if (limit != null) {
            newLimit = shuttle.apply(limit);
        }
        return copy(input, newOrders, newLimit, (PathRecordType) getRowType());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        if (!orderByExpressions.isEmpty()) {
            writer.item("order by", orderByExpressions);
        }
        if (limit != null) {
            writer.item("limit", limit);
        }
        return writer;
    }
}
