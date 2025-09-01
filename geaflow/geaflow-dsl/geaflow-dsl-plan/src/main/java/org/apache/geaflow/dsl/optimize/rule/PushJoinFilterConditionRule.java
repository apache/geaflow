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

package org.apache.geaflow.dsl.optimize.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

public class PushJoinFilterConditionRule extends RelOptRule {

    public static final PushJoinFilterConditionRule INSTANCE = new PushJoinFilterConditionRule();

    private PushJoinFilterConditionRule() {
        super(operand(LogicalFilter.class,
            operand(LogicalJoin.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalJoin join = call.rel(1);
        if (!join.getJoinType().equals(JoinRelType.INNER)) {
            // Consecutive pushing of conditions for non-INNER joins is not supported.
            return;
        }
        List<RexNode> splitRexNodes = RelOptUtil.conjunctions(filter.getCondition());
        final List<RexNode> joinRexNodes = new ArrayList<>();
        final List<RexNode> remainRexNodes = new ArrayList<>();
        splitRexNodes.stream().map(n -> {
            if (n.getKind().equals(SqlKind.EQUALS)) {
                joinRexNodes.add(n);
            } else {
                remainRexNodes.add(n);
            }
            return n;
        }).collect(Collectors.toList());
        joinRexNodes.add(join.getCondition());
        RexNode equalRexNode = RexUtil.composeConjunction(
            new RexBuilder(call.builder().getTypeFactory()), joinRexNodes);
        RexNode remainRexNode = RexUtil.composeConjunction(
            new RexBuilder(call.builder().getTypeFactory()), remainRexNodes);
        LogicalJoin newJoin = join.copy(join.getTraitSet(), equalRexNode, join.getLeft(),
            join.getRight(), join.getJoinType(), join.isSemiJoinDone());
        if (remainRexNode.isAlwaysTrue()) {
            call.transformTo(newJoin);
        } else {
            call.transformTo(filter.copy(filter.getTraitSet(), newJoin, remainRexNode));
        }
    }
}
