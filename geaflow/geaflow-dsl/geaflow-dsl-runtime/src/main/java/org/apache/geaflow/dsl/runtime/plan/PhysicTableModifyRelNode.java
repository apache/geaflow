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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.SinkDataView;
import org.apache.geaflow.dsl.schema.GeaFlowTable;

public class PhysicTableModifyRelNode extends TableModify implements PhysicRelNode<SinkDataView> {

    public PhysicTableModifyRelNode(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RelOptTable table,
                                    Prepare.CatalogReader catalogReader,
                                    RelNode input,
                                    Operation operation,
                                    List<String> updateColumnList,
                                    List<RexNode> sourceExpressionList,
                                    boolean flattened) {
        super(cluster, traitSet, table, catalogReader, input, operation,
            updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicTableModifyRelNode(super.getCluster(),
            traitSet,
            super.getTable(),
            super.catalogReader,
            sole(inputs),
            super.getOperation(),
            super.getUpdateColumnList(),
            super.getSourceExpressionList(),
            super.isFlattened());
    }

    @SuppressWarnings("unchecked")
    @Override
    public SinkDataView translate(QueryContext context) {
        GeaFlowTable table = getTable().unwrap(GeaFlowTable.class);
        context.addReferTargetTable(table);

        RuntimeTable runtimeTable = ((PhysicRelNode<RuntimeTable>) getInput()).translate(context);
        return runtimeTable.write(table);
    }

    @Override
    public String showSQL() {
        GeaFlowTable table = super.getTable().unwrap(GeaFlowTable.class);
        return table.toString();
    }
}
