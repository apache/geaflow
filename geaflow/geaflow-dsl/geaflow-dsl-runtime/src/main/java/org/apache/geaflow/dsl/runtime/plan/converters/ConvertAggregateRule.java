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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.geaflow.dsl.runtime.plan.PhysicAggregateRelNode;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;

public class ConvertAggregateRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertAggregateRule();

    private ConvertAggregateRule() {
        super(LogicalAggregate.class, Convention.NONE, PhysicConvention.INSTANCE,
            ConvertAggregateRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);
        Preconditions.checkArgument(aggregate.getGroupType() == Aggregate.Group.SIMPLE,
            "Only support Aggregate.Group.SIMPLE, current group type " + aggregate.getGroupType());
        return true;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate aggregate = (LogicalAggregate) rel;

        RelTraitSet relTraitSet = aggregate.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(aggregate.getInput(), aggregate.getInput().getTraitSet()
            .replace(PhysicConvention.INSTANCE));

        return new PhysicAggregateRelNode(aggregate.getCluster(), relTraitSet,
            convertedInput, aggregate.indicator, aggregate.getGroupSet(),
            aggregate.getGroupSets(), aggregate.getAggCallList());
    }
}
