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

package org.apache.geaflow.dsl.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.rel.GraphAlgorithm;

public class LogicalGraphAlgorithm extends GraphAlgorithm {

    protected LogicalGraphAlgorithm(RelOptCluster cluster,
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
        return create(input.getCluster(), traitSet, input, userFunctionClass, params);
    }

    @Override
    public GraphAlgorithm copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        RelNode input = inputs.get(0);
        return new LogicalGraphAlgorithm(input.getCluster(), traitSet, input, userFunctionClass, params);
    }

    public static LogicalGraphAlgorithm create(RelOptCluster cluster,
                                               RelTraitSet traits,
                                               RelNode input,
                                               Class<? extends AlgorithmUserFunction> userFunctionClass,
                                               Object[] params) {
        return new LogicalGraphAlgorithm(cluster, traits, input, userFunctionClass, params);
    }
}
