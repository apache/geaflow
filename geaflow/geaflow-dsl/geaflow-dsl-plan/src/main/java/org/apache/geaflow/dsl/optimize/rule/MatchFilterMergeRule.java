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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class MatchFilterMergeRule extends RelOptRule {

    public static final MatchFilterMergeRule INSTANCE = new MatchFilterMergeRule();

    private MatchFilterMergeRule() {
        super(operand(MatchFilter.class,
            operand(MatchFilter.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchFilter topFilter = call.rel(0);
        MatchFilter bottomFilter = call.rel(1);

        RexNode mergedCondition = GQLRexUtil.and(
            Lists.newArrayList(topFilter.getCondition(), bottomFilter.getCondition()),
            call.builder().getRexBuilder());

        MatchFilter mergedFilter = MatchFilter.create(bottomFilter.getInput(),
            mergedCondition, bottomFilter.getPathSchema());
        call.transformTo(mergedFilter);
    }
}
