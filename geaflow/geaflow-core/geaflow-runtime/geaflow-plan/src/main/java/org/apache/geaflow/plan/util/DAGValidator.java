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

package org.apache.geaflow.plan.util;

import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;

public class DAGValidator {

    /**
     * Gets and verifies whether the upstream vertex of the current vertex in the dag exists.
     *
     * @param pipelineGraph  The pipeline plan.
     * @param pipelineVertex The current vertex.
     */
    public static void checkVertexValidity(PipelineGraph pipelineGraph, PipelineVertex pipelineVertex, boolean fetchPrevious) {
        for (PipelineEdge pipelineEdge : pipelineGraph.getPipelineEdgeList()) {
            int vertexId;
            if (fetchPrevious) {
                vertexId = pipelineEdge.getTargetId();
            } else {
                vertexId = pipelineEdge.getSrcId();
            }

            // Input vertex check, for chain and non-chain mode.
            if (pipelineVertex.getVertexId() == vertexId) {
                int previousChainTailVertexId = pipelineEdge.getPartition().getOpId();
                PipelineVertex previousVertex = null;
                if (pipelineGraph.getVertexMap().containsKey(previousChainTailVertexId)) {
                    previousVertex = pipelineGraph.getVertexMap().get(previousChainTailVertexId);
                }
                // Maybe encounter the situation that previous vertex is null.
                if (previousVertex == null) {
                    throw new GeaflowRuntimeException(RuntimeErrors.INST
                        .previousVertexIsNullError(String.valueOf(pipelineVertex.getVertexId())));
                }
            }
        }
    }
}
