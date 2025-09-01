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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;

public class FilterToMatchRule extends RelOptRule {

    public static final FilterToMatchRule INSTANCE = new FilterToMatchRule();

    private FilterToMatchRule() {
        super(operand(LogicalFilter.class,
            operand(LogicalGraphMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalGraphMatch graphMatch = call.rel(1);

        IMatchNode pathPattern = graphMatch.getPathPattern();

        MatchFilter matchFilter = MatchFilter.create(pathPattern,
            filter.getCondition(), pathPattern.getPathSchema());
        LogicalGraphMatch newMatch = (LogicalGraphMatch) graphMatch.copy(matchFilter);
        call.transformTo(newMatch);
    }
}
