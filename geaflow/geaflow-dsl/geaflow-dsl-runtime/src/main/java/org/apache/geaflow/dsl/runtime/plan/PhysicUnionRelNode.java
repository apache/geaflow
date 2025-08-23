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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RDataView.ViewType;
import org.apache.geaflow.dsl.runtime.RuntimeTable;

/**
 * UNION.
 */
public class PhysicUnionRelNode extends Union implements PhysicRelNode<RuntimeTable> {

    public PhysicUnionRelNode(RelOptCluster cluster,
                              RelTraitSet traits,
                              List<RelNode> inputs,
                              boolean all) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new PhysicUnionRelNode(super.getCluster(), traitSet, inputs, all);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        List<RDataView> dataViews = new ArrayList<>();
        for (RelNode input : getInputs()) {
            dataViews.add(((PhysicRelNode<?>) input).translate(context));
            if (dataViews.get(dataViews.size() - 1).getType() != ViewType.TABLE) {
                throw new GeaFlowDSLException("DataView: "
                    + dataViews.get(dataViews.size() - 1).getType() + " cannot support SQL union");
            }
        }
        if (!dataViews.isEmpty()) {
            RuntimeTable output = (RuntimeTable) dataViews.get(0);
            for (int i = 1; i < dataViews.size(); i++) {
                output = output.union((RuntimeTable) dataViews.get(i));
            }
            return output;
        } else {
            throw new GeaFlowDSLException("Union inputs cannot be empty.");
        }
    }

    @Override
    public String showSQL() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getInputs().size(); i++) {
            if (i > 0) {
                sb.append(" ").append(kind.name()).append(" ");
            }
            sb.append(((PhysicRelNode) getInputs().get(i)).showSQL());
        }
        return sb.toString();
    }

}
