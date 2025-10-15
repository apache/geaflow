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
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.rel.GraphAlgorithm;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;

public class PhysicGraphAlgorithm extends GraphAlgorithm implements PhysicRelNode<RuntimeTable> {

    public PhysicGraphAlgorithm(RelOptCluster cluster,
                                RelTraitSet traits,
                                RelNode input,
                                Class<? extends AlgorithmUserFunction> userFunctionClass,
                                Object[] params) {
        super(cluster, traits, input, userFunctionClass, params);
    }

    @Override
    public GraphAlgorithm copy(RelTraitSet traitSet, RelNode input,
                               Class<? extends AlgorithmUserFunction> userFunctionClass,
                               Object[] params) {
        return new PhysicGraphAlgorithm(input.getCluster(), traitSet, input, userFunctionClass, params);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        PhysicRelNode<RuntimeGraph> input = (PhysicRelNode<RuntimeGraph>) getInput();
        RuntimeGraph inputGraph = input.translate(context);
        return inputGraph.runAlgorithm(this);
    }

    @Override
    public String showSQL() {
        return null;
    }
}
