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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RuntimeTable;

public class PhysicExchangeRelNode extends Exchange implements PhysicRelNode<RuntimeTable> {

    public PhysicExchangeRelNode(RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 RelNode input,
                                 RelDistribution distribution) {
        super(cluster, traitSet, input, distribution);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Exchange copy(RelTraitSet traitSet, RelNode input,
                         RelDistribution distribution) {
        return new PhysicExchangeRelNode(super.getCluster(),
            traitSet, input, distribution);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        return null;
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();

        sql.append("PARTITION BY ");
        RelDataType inputRowType = getInput().getRowType();
        for (int i = 0; i < distribution.getKeys().size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            int key = distribution.getKeys().get(i);
            sql.append(inputRowType.getFieldNames().get(key));
        }
        return sql.toString();
    }
}
