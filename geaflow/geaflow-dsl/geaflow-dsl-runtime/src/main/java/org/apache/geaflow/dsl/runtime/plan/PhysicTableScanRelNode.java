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

package org.apache.geaflow.dsl.runtime.plan;

import java.util.List;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.schema.GeaFlowTable;

public class PhysicTableScanRelNode extends TableScan implements PhysicRelNode<RuntimeTable> {

    public PhysicTableScanRelNode(RelOptCluster cluster,
                                  RelTraitSet traitSet,
                                  RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicTableScanRelNode(getCluster(), traitSet, getTable());
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        GeaFlowTable geaFlowTable = table.unwrap(GeaFlowTable.class);
        RuntimeTable runtimeTable = context.getRuntimeTable(geaFlowTable.getName());
        if (runtimeTable == null) {
            Expression pushFilter = context.getPushFilter();
            context.addReferSourceTable(geaFlowTable);
            runtimeTable = context.getEngineContext().createRuntimeTable(context, geaFlowTable, pushFilter);
            context.putRuntimeTable(geaFlowTable.getName(), runtimeTable);
        }
        return runtimeTable;
    }

    @Override
    public String showSQL() {
        GeaFlowTable table = super.getTable().unwrap(GeaFlowTable.class);
        return table.toString();
    }
}
