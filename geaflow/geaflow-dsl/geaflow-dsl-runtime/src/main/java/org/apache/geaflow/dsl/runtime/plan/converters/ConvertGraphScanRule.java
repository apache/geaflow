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
import org.apache.geaflow.dsl.rel.logical.LogicalGraphScan;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicGraphScanRelNode;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;

public class ConvertGraphScanRule extends ConverterRule {

    public static final ConvertGraphScanRule INSTANCE = new ConvertGraphScanRule();

    private ConvertGraphScanRule() {
        super(LogicalGraphScan.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertGraphScanRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalGraphScan scan = call.rel(0);
        GeaFlowGraph table = scan.getTable().unwrap(GeaFlowGraph.class);
        return table != null;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalGraphScan graphScan = (LogicalGraphScan) rel;

        RelTraitSet relTraitSet = graphScan.getTraitSet().replace(PhysicConvention.INSTANCE);
        return new PhysicGraphScanRelNode(graphScan.getCluster(), relTraitSet, graphScan.getTable());
    }
}
