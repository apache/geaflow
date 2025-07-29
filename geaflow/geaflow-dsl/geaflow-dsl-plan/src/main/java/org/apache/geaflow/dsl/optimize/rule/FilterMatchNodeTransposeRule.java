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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.match.IMatchLabel;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rex.RexLambdaCall;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class FilterMatchNodeTransposeRule extends RelOptRule {

    public static final FilterMatchNodeTransposeRule INSTANCE = new FilterMatchNodeTransposeRule();

    private FilterMatchNodeTransposeRule() {
        super(operand(MatchFilter.class,
            operand(IMatchLabel.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchFilter filter = call.rel(0);
        IMatchLabel matchLabel = call.rel(1);
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());

        List<RexNode> pushes = new ArrayList<>();
        List<RexNode> remains = new ArrayList<>();
        for (RexNode condition : conditions) {
            if (canPush(condition, matchLabel)) {
                pushes.add(condition);
            } else {
                remains.add(condition);
            }
        }
        if (pushes.isEmpty()) {
            return;
        }
        RexBuilder builder = call.builder().getRexBuilder();
        MatchFilter pushFilter = filter.copy(filter.getTraitSet(),
            matchLabel.getInput(),
            GQLRexUtil.and(pushes, builder),
            (PathRecordType) matchLabel.getInput().getRowType());
        IMatchNode newMatchNode = (IMatchNode) matchLabel.copy(matchLabel.getTraitSet(),
            Lists.newArrayList(pushFilter));

        if (remains.isEmpty()) {
            call.transformTo(newMatchNode);
        } else {
            MatchFilter remainFiler = MatchFilter.create(newMatchNode,
                GQLRexUtil.and(remains, builder),
                filter.getPathSchema());
            call.transformTo(remainFiler);
        }
    }

    private boolean canPush(RexNode condition, IMatchLabel match) {
        if (GQLRexUtil.contain(condition, RexLambdaCall.class)) {
            return false;
        }
        List<RexFieldAccess> fieldAccesses = GQLRexUtil.collect(condition, node -> node instanceof RexFieldAccess);
        for (RexFieldAccess fieldAccess : fieldAccesses) {
            if (fieldAccess.getReferenceExpr() instanceof RexInputRef) {
                RexInputRef pathRef = (RexInputRef) fieldAccess.getReferenceExpr();
                if (isRefCurrentNode(pathRef, match)) {
                    return false;
                }
            }
        }
        List<RexInputRef> pathRefs = GQLRexUtil.collect(condition, node -> node instanceof RexInputRef);
        for (RexInputRef pathRef : pathRefs) {
            if (isRefCurrentNode(pathRef, match)) {
                return false;
            }
        }
        return true;
    }

    private boolean isRefCurrentNode(RexInputRef pathRef, IMatchNode match) {
        // Test if the condition has referred current match node.
        // We cannot push the condition down as current match node has been referred by the condition.
        return pathRef.getIndex() == match.getPathSchema().getFieldCount() - 1;
    }
}
