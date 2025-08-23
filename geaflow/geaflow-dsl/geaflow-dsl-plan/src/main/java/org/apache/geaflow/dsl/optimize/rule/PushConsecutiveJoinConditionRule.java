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
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class PushConsecutiveJoinConditionRule extends RelOptRule {

    public static final PushConsecutiveJoinConditionRule INSTANCE = new PushConsecutiveJoinConditionRule();

    private PushConsecutiveJoinConditionRule() {
        super(operand(LogicalJoin.class, operand(LogicalJoin.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin bottomJoin = call.rel(0);
        LogicalJoin join = call.rel(1);
        if (!join.getJoinType().equals(JoinRelType.INNER) || !bottomJoin.getJoinType().equals(JoinRelType.INNER)) {
            // Consecutive pushing of conditions for non-INNER joins is not supported.
            return;
        }
        List<RexNode> splitRexNodes = RelOptUtil.conjunctions(bottomJoin.getCondition());
        List<RexNode> pushRexNodes = splitRexNodes.stream().filter(n -> n.getKind().equals(SqlKind.EQUALS)).collect(
            Collectors.toList());
        // Filter out conditions unrelated to the sub-join.
        boolean isBottomLeft = GQLRelUtil.toRel(bottomJoin.getLeft()).equals(join);
        int fieldCount = join.getRowType().getFieldCount();
        int fieldStart = isBottomLeft ? 0 : bottomJoin.getRowType().getFieldCount() - fieldCount;
        int fieldEnd = isBottomLeft ? fieldCount : bottomJoin.getRowType().getFieldCount();
        pushRexNodes = pushRexNodes.stream().filter(
            rex -> GQLRexUtil.collect(rex, r -> r instanceof RexInputRef).stream().noneMatch(
                inputRef -> ((RexInputRef) inputRef).getIndex() < fieldStart
                    || ((RexInputRef) inputRef).getIndex() >= fieldEnd
            )
        ).collect(Collectors.toList());
        List<RexNode> remainRexNodes = new ArrayList<>();
        for (RexNode rex : splitRexNodes) {
            if (!pushRexNodes.contains(rex)) {
                remainRexNodes.add(rex);
            }
        }
        // If the join is the right input, adjust all referenced indices.
        if (!isBottomLeft) {
            pushRexNodes = pushRexNodes.stream().map(
                rex -> GQLRexUtil.replace(rex, rexNode -> {
                    if (rexNode instanceof RexInputRef) {
                        int index = ((RexInputRef) rexNode).getIndex();
                        index -= (bottomJoin.getRowType().getFieldCount() - fieldCount);
                        return new RexInputRef(index, rexNode.getType());
                    } else {
                        return rexNode;
                    }
                })
            ).collect(Collectors.toList());
        }
        pushRexNodes.add(join.getCondition());
        RexNode equalRexNode = RexUtil.composeConjunction(
            call.builder().getRexBuilder(), pushRexNodes);
        LogicalJoin newJoin = join.copy(join.getTraitSet(), equalRexNode, join.getLeft(),
            join.getRight(), join.getJoinType(), join.isSemiJoinDone());
        call.transformTo(bottomJoin.copy(bottomJoin.getTraitSet(),
            RexUtil.composeConjunction(call.builder().getRexBuilder(), remainRexNodes),
            isBottomLeft ? newJoin : bottomJoin.getLeft(),
            isBottomLeft ? bottomJoin.getRight() : newJoin,
            bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone()));
    }
}
