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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicViewRelNode;
import org.apache.geaflow.dsl.schema.GeaFlowView;

public class ConvertViewRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertViewRule();

    private ConvertViewRule() {
        super(LogicalTableScan.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertViewRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);
        GeaFlowView view = scan.getTable().unwrap(GeaFlowView.class);
        return view != null;
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalTableScan scan = (LogicalTableScan) relNode;
        RelTraitSet relTraitSet = relNode.getTraitSet().replace(PhysicConvention.INSTANCE);

        return new PhysicViewRelNode(relNode.getCluster(), relTraitSet, scan.getTable());
    }

}
