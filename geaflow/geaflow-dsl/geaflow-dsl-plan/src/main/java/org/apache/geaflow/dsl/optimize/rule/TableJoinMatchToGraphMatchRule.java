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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class TableJoinMatchToGraphMatchRule extends AbstractJoinToGraphRule {

    public static final TableJoinMatchToGraphMatchRule INSTANCE = new TableJoinMatchToGraphMatchRule();

    private TableJoinMatchToGraphMatchRule() {
        super(operand(LogicalJoin.class,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        if (!isSupportJoinType(join.getJoinType())) {
            // non-INNER joins is not supported.
            return false;
        }
        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);
        return isSingleChainFromLogicalTableScan(leftInput)
            && isSingleChainFromGraphMatch(rightInput);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode leftInput = call.rel(1);
        RelNode leftTableScan = leftInput;
        while (!(leftTableScan instanceof LogicalTableScan)) {
            leftTableScan = GQLRelUtil.toRel(leftTableScan.getInput(0));
        }
        RelNode rightInput = call.rel(2);
        RelNode graphMatchProject = null;
        RelNode rightGraphMatch = rightInput;
        while (rightGraphMatch != null && !(rightGraphMatch instanceof LogicalGraphMatch)) {
            graphMatchProject = rightGraphMatch;
            rightGraphMatch = GQLRelUtil.toRel(rightGraphMatch.getInput(0));
        }
        RelNode tail = processGraphMatchJoinTable(
            call,
            call.rel(0),
            (LogicalGraphMatch) rightGraphMatch,
            (LogicalProject) graphMatchProject,
            (LogicalTableScan) leftTableScan,
            leftInput, leftTableScan, rightInput, rightGraphMatch, false
        );
        if (tail == null) {
            return;
        }
        LogicalJoin join = call.rel(0);
        // add remain filter.
        JoinInfo joinInfo = join.analyzeCondition();
        RexNode remainFilter = joinInfo.getRemaining(join.getCluster().getRexBuilder());
        if (remainFilter != null && !remainFilter.isAlwaysTrue()) {
            tail = LogicalFilter.create(tail, remainFilter);
        }
        call.transformTo(tail);
    }
}
