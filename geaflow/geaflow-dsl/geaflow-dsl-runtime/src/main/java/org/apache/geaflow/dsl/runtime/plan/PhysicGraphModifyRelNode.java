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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.rel.GraphModify;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RDataView.ViewType;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.SinkDataView;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;

public class PhysicGraphModifyRelNode extends GraphModify implements PhysicRelNode<SinkDataView> {

    public PhysicGraphModifyRelNode(RelOptCluster cluster, RelTraitSet traitSet,
                                    GeaFlowGraph graph, RelNode input) {
        super(cluster, traitSet, graph, input);
    }

    @Override
    public GraphModify copy(RelTraitSet traitSet, GeaFlowGraph graph, RelNode input) {
        return new PhysicGraphModifyRelNode(getCluster(), traitSet, graph, input);
    }

    @Override
    public SinkDataView translate(QueryContext context) {
        context.addReferTargetGraph(graph);

        RDataView dataView = ((PhysicRelNode<?>) getInput()).translate(context);
        if (dataView.getType() == ViewType.TABLE) {
            RuntimeTable runtimeTable = (RuntimeTable) dataView;
            return runtimeTable.write(graph, context);
        }
        throw new GeaFlowDSLException("DataView type: {} cannot insert to graph", dataView.getType());
    }

    @Override
    public String showSQL() {
        return null;
    }
}
