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
import org.apache.geaflow.dsl.rel.logical.LogicalGraphModify;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicGraphModifyRelNode;

public class ConvertGraphModifyRule extends ConverterRule {

    public static final ConvertGraphModifyRule INSTANCE = new ConvertGraphModifyRule();

    private ConvertGraphModifyRule() {
        super(LogicalGraphModify.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertGraphModifyRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalGraphModify graphModify = (LogicalGraphModify) rel;

        RelTraitSet relTraitSet = graphModify.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(graphModify.getInput(),
            graphModify.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicGraphModifyRelNode(
            graphModify.getCluster(),
            relTraitSet,
            graphModify.getGraph(),
            convertedInput);
    }
}
