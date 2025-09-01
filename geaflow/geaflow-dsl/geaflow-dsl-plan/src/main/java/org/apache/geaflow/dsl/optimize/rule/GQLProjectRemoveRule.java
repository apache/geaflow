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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class GQLProjectRemoveRule extends ProjectRemoveRule {
    public static final GQLProjectRemoveRule INSTANCE =
        new GQLProjectRemoveRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a ProjectRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public GQLProjectRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Project project = call.rel(0);
        if (GQLRelUtil.isGQLMatchRelNode(project.getInput())) {
            return false;
        }
        return super.matches(call);
    }
}
