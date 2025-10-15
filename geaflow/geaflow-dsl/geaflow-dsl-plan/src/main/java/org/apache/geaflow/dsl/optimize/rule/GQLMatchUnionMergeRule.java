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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.rel.match.MatchUnion;

public class GQLMatchUnionMergeRule extends RelOptRule {

    public static final GQLMatchUnionMergeRule INSTANCE = new GQLMatchUnionMergeRule();

    private GQLMatchUnionMergeRule() {
        super(operand(MatchUnion.class, operand(MatchUnion.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchUnion topMatchUnion = call.rel(0);
        MatchUnion bottomMatchUnion = call.rel(1);
        if (topMatchUnion.isDistinct()
            || !bottomMatchUnion.isDistinct() && !topMatchUnion.isDistinct()) { //distinct
            List<RelNode> newInputs = new ArrayList<>();
            newInputs.addAll(bottomMatchUnion.getInputs());
            for (int i = 1; i < topMatchUnion.getInputs().size(); i++) {
                newInputs.add(topMatchUnion.getInput(i));
            }
            MatchUnion newMatchUnion = MatchUnion.create(topMatchUnion.getCluster(),
                topMatchUnion.getTraitSet(), newInputs, topMatchUnion.all);
            call.transformTo(newMatchUnion);
        }
    }
}
