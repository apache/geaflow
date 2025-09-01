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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType.VirtualVertexRecordType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.VirtualEdgeMatch;
import org.apache.geaflow.dsl.rex.RexLambdaCall;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class AddVertexResetRule extends RelOptRule {

    public static final AddVertexResetRule INSTANCE = new AddVertexResetRule();

    private AddVertexResetRule() {
        super(operand(SingleMatchNode.class,
            operand(IMatchNode.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SingleMatchNode matchNode = call.rel(0);
        IMatchNode inputNode = call.rel(1);
        // If the match node contains sub-query and the input output type is virtual vertex type.
        // we should reset the latest vertex to it's task for sub query calling.
        List<RexLambdaCall> lambdaCalls = GQLRexUtil.collect(matchNode,
            rexNode -> rexNode instanceof RexLambdaCall);
        if (lambdaCalls.isEmpty()) {
            return;
        }
        if (!(inputNode.getNodeType() instanceof VirtualVertexRecordType)) {
            return;
        }

        Set<Integer> startVertices = new HashSet<>();

        for (RexLambdaCall lambdaCall : lambdaCalls) {
            int startVertex = getStartVertexIndex(lambdaCall);
            startVertices.add(startVertex);
        }
        if (startVertices.size() > 1) {
            return;
        }
        PathRecordType inputPathType = inputNode.getPathSchema();
        // start vertex should be the last field in the input path type.
        if (startVertices.iterator().next() != inputPathType.getFieldCount() - 1) {
            return;
        }
        int startVertexIndex = inputPathType.getFieldCount() - 1;
        RexInputRef startVertexRef = call.builder().getRexBuilder()
            .makeInputRef(inputPathType.getFieldList().get(startVertexIndex).getType(),
                startVertexIndex);

        RexNode startVertexIdRef = call.builder().getRexBuilder()
            .makeFieldAccess(startVertexRef, VertexType.ID_FIELD_POSITION);
        VirtualEdgeMatch virtualEdgeMatch = VirtualEdgeMatch.create(inputNode, startVertexIdRef, inputPathType);
        RelNode newMatchNode = matchNode.copy(matchNode.getTraitSet(),
            Collections.singletonList(virtualEdgeMatch));
        call.transformTo(newMatchNode);
    }

    private int getStartVertexIndex(RexLambdaCall lambdaCall) {
        SingleMatchNode matchNode = (SingleMatchNode) lambdaCall.getInput().rel;
        return GQLRelUtil.getFirstMatchNode(matchNode).getPathSchema().getFieldCount() - 1;
    }
}
