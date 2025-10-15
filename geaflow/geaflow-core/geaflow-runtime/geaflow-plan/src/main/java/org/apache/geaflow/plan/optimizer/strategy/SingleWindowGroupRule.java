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

package org.apache.geaflow.plan.optimizer.strategy;

import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleWindowGroupRule implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleWindowGroupRule.class);

    /**
     * Apply group rule in plan.
     */
    public void apply(PipelineGraph pipelineGraph) {
        List<PipelineVertex> sourceVertexList = pipelineGraph.getSourceVertices();
        // 1. Check whether is single window mode.
        boolean isSingleWindow = sourceVertexList.stream().allMatch(v ->
            ((AbstractOperator) v.getOperator()).getOpArgs().getOpType() == OpArgs.OpType.SINGLE_WINDOW_SOURCE);

        // 2. Apply no group rule.
        if (isSingleWindow) {
            pipelineGraph.getPipelineVertices().stream().forEach(
                v -> ((AbstractOperator) v.getOperator()).getOpArgs().setEnGroup(false));
            LOGGER.info("apply no group rule success");
        }
    }

}
