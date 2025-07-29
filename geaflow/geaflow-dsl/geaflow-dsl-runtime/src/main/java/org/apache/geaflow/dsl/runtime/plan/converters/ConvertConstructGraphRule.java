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
import org.apache.geaflow.dsl.rel.logical.LogicalConstructGraph;
import org.apache.geaflow.dsl.runtime.plan.PhysicConstructGraphRelNode;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;

public class ConvertConstructGraphRule extends ConverterRule {

    public static final ConvertConstructGraphRule INSTANCE = new ConvertConstructGraphRule();

    private ConvertConstructGraphRule() {
        super(LogicalConstructGraph.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertConstructGraphRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalConstructGraph constructGraph = (LogicalConstructGraph) rel;

        RelTraitSet relTraitSet = constructGraph.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(constructGraph.getInput(),
            constructGraph.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicConstructGraphRelNode(constructGraph.getCluster(), relTraitSet, convertedInput,
            constructGraph.getLabelNames(), constructGraph.getRowType());
    }
}
