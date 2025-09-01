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

package org.apache.geaflow.dsl.runtime.plan.converters;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicGraphMatchRelNode;

public class ConvertGraphMatchRule extends ConverterRule {

    public static final ConvertGraphMatchRule INSTANCE = new ConvertGraphMatchRule();

    private ConvertGraphMatchRule() {
        super(LogicalGraphMatch.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertGraphMatchRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalGraphMatch graphMatch = (LogicalGraphMatch) rel;

        RelTraitSet relTraitSet = graphMatch.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(graphMatch.getInput(),
            graphMatch.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicGraphMatchRelNode(graphMatch.getCluster(), relTraitSet,
            convertedInput, graphMatch.getPathPattern(), graphMatch.getRowType());
    }
}
