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

package org.apache.geaflow.plan.optimizer;

import java.io.Serializable;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.optimizer.strategy.ChainCombiner;
import org.apache.geaflow.plan.optimizer.strategy.LocalShuffleOptimizer;
import org.apache.geaflow.plan.optimizer.strategy.SingleWindowGroupRule;

public class PipelineGraphOptimizer implements Serializable {

    public void optimizePipelineGraph(PipelineGraph pipelineGraph) {
        // 1. Enforce chain combiner optimization.
        // Merge operators with forward partition into single execution unit.
        ChainCombiner chainCombiner = new ChainCombiner();
        chainCombiner.combineVertex(pipelineGraph);

        // 2. Enforce local shuffle optimization for graph → sink/map patterns.
        // Mark vertices for co-location to enable automatic local shuffle.
        LocalShuffleOptimizer localShuffleOptimizer = new LocalShuffleOptimizer();
        localShuffleOptimizer.optimize(pipelineGraph);

        // 3. Enforce single window rule.
        // Disable grouping for single-window batch jobs.
        SingleWindowGroupRule groupRule = new SingleWindowGroupRule();
        groupRule.apply(pipelineGraph);
    }
}
