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

import static org.apache.geaflow.dsl.util.GQLRelUtil.toRel;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchPathSort;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchSortToLogicalSortRule extends RelOptRule {

    public static final MatchSortToLogicalSortRule INSTANCE = new MatchSortToLogicalSortRule();

    private MatchSortToLogicalSortRule() {
        super(operand(RelNode.class,
            operand(LogicalGraphMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode topNode = call.rel(0);
        LogicalGraphMatch graphMatch = call.rel(1);

        if (!(graphMatch.getPathPattern() instanceof MatchPathSort)
            || GQLRelUtil.isGQLMatchRelNode(topNode)) {
            return;
        }
        MatchPathSort pathSort = (MatchPathSort) graphMatch.getPathPattern();
        RexBuilder rexBuilder = call.builder().getRexBuilder();

        List<RexNode> projects = new ArrayList<>();
        RexNode[] orderRexNodes = new RexNode[pathSort.getOrderByExpressions().size()];

        RelDataType inputType = pathSort.getInput().getRowType();
        List<Integer> topReferences = GQLRelUtil.getRelNodeReference(topNode);
        Map<Integer, Integer> topReferMapping = new HashMap<>();

        for (int i = 0; i < topReferences.size(); i++) {
            int topRefer = topReferences.get(i);
            projects.add(rexBuilder.makeInputRef(inputType.getFieldList().get(topRefer).getType(), topRefer));
            topReferMapping.put(topRefer, i);
        }
        for (int i = 0; i < pathSort.getOrderByExpressions().size(); i++) {
            RexNode orderExp = pathSort.getOrderByExpressions().get(i);
            projects.add(orderExp);

            RexNode orderRex = rexBuilder.makeInputRef(orderExp.getType(), i + topReferences.size());
            if (orderExp.getKind() == SqlKind.DESCENDING) {
                orderRex = rexBuilder.makeCall(SqlStdOperatorTable.DESC, orderRex);
            }
            orderRexNodes[i] = orderRex;
        }

        RelNode newTop;
        graphMatch = (LogicalGraphMatch) graphMatch.copy((IMatchNode) toRel(pathSort.getInput()));
        LogicalSort logicalSort;
        if (orderRexNodes.length > 0) {
            logicalSort = (LogicalSort) call.builder().push(graphMatch)
                .project(projects)
                .sort(orderRexNodes)
                .build();
            logicalSort = (LogicalSort) logicalSort.copy(logicalSort.getTraitSet(),
                logicalSort.getInput(),
                logicalSort.getCollation(),
                null,
                pathSort.getLimit());
            // adjust the reference index for topNode after add project to the graph match
            newTop = GQLRelUtil.adjustInputRef(topNode, topReferMapping);
        } else {
            logicalSort = LogicalSort.create(graphMatch, RelCollations.EMPTY,
                null, pathSort.getLimit());
            newTop = topNode;
        }

        newTop = newTop.copy(topNode.getTraitSet(), Lists.newArrayList(logicalSort));
        call.transformTo(newTop);
    }
}
