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

package org.apache.geaflow.dsl.rel.match;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.rex.MatchAggregateCall;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchAggregate extends SingleRel implements SingleMatchNode {

    public final boolean indicator;
    protected final List<MatchAggregateCall> aggCalls;
    protected final List<RexNode> groupSet;

    protected MatchAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                             boolean indicator, List<RexNode> groupSet,
                             List<MatchAggregateCall> aggCalls, PathRecordType pathType) {
        super(cluster, traits, input);
        this.indicator = indicator; // true is allowed, but discouraged
        this.aggCalls = ImmutableList.copyOf(aggCalls);
        this.groupSet = Objects.requireNonNull(groupSet);
        for (MatchAggregateCall aggCall : aggCalls) {
            Preconditions.checkArgument(aggCall.filterArg < 0
                    || isPredicate(input, aggCall.filterArg),
                "filter must be BOOLEAN NOT NULL");
        }
        this.rowType = Objects.requireNonNull(pathType);
    }

    private boolean isPredicate(RelNode input, int index) {
        final RelDataType type =
            input.getRowType().getFieldList().get(index).getType();
        return type.getSqlTypeName() == SqlTypeName.BOOLEAN
            && !type.isNullable();
    }

    public List<RexNode> getGroupSet() {
        return groupSet;
    }

    public List<MatchAggregateCall> getAggCalls() {
        return aggCalls;
    }

    public boolean isIndicator() {
        return indicator;
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) rowType;
    }

    @Override
    public RelDataType getNodeType() {
        return ((IMatchNode) GQLRelUtil.toRel(getInput())).getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitAggregate(this);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathType) {
        return new MatchAggregate(getCluster(), getTraitSet(), sole(inputs),
            indicator, groupSet, aggCalls, (PathRecordType) rowType);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MatchAggregate(getCluster(), traitSet, sole(inputs),
            indicator, groupSet, aggCalls, (PathRecordType) rowType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<RexNode> rewriteGroupList = this.groupSet.stream().map(rex -> rex.accept(shuttle))
            .collect(Collectors.toList());
        List<MatchAggregateCall> rewriteAggCalls = this.aggCalls.stream().map(call -> {
            List<RexNode> rewriteArgList = call.getArgList().stream()
                .map(shuttle::apply).collect(Collectors.toList());
            return new MatchAggregateCall(call.getAggregation(), call.isDistinct(),
                call.isApproximate(), rewriteArgList, call.filterArg, call.getCollation(),
                call.getType(), call.getName());
        }).collect(Collectors.toList());
        return MatchAggregate.create(getInput(), indicator, rewriteGroupList, rewriteAggCalls,
            (PathRecordType) rowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        if (!groupSet.isEmpty()) {
            writer.item("group by", groupSet);
        }
        if (aggCalls != null) {
            writer.item("aggCalls", aggCalls);
        }
        return writer;
    }

    public static MatchAggregate create(RelNode input, boolean indicator, List<RexNode> groupSet,
                                        List<MatchAggregateCall> aggCalls, PathRecordType pathType) {
        return new MatchAggregate(input.getCluster(), input.getTraitSet(), input, indicator,
            groupSet, aggCalls, pathType);
    }
}
