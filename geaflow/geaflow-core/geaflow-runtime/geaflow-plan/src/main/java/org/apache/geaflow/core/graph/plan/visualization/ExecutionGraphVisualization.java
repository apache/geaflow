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

package org.apache.geaflow.core.graph.plan.visualization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.core.graph.ExecutionGraph;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.core.graph.ExecutionVertexGroupEdge;

public class ExecutionGraphVisualization {

    public static final String SCHEDULER = "scheduler";
    private static final String NODE_FORMAT = "%s [label=\"%s\"]\n";
    private static final String EMPTY_GRAPH = new StringBuilder("digraph G {\n")
        .append("0 [label=\"node-0\"]\n")
        .append(String.format(NODE_FORMAT, SCHEDULER, SCHEDULER))
        .append("}")
        .toString();

    private ExecutionGraph executionGraph;
    private List<ExecutionVertexGroupEdge> vertexGroupEdgeList;
    private Map<Integer, ExecutionVertexGroup> vertexGroupMap;

    public ExecutionGraphVisualization(ExecutionGraph executionGraph) {
        this.executionGraph = executionGraph;
        this.vertexGroupEdgeList = new ArrayList<>(executionGraph.getGroupEdgeMap().values());
        this.vertexGroupMap = executionGraph.getVertexGroupMap();
    }

    public String getExecutionGraphViz() {
        if (vertexGroupEdgeList.size() == 0 && vertexGroupMap.size() == 0) {
            return EMPTY_GRAPH;
        }

        Collections.sort(vertexGroupEdgeList, (o1, o2) -> {
            int i = Integer.compare(o1.getGroupSrcId(), o2.getGroupSrcId());
            if (i == 0) {
                return Integer.compare(o1.getGroupTargetId(), o2.getGroupTargetId());
            } else {
                return i;
            }
        });

        StringBuilder builder = new StringBuilder("digraph G {\n");
        for (ExecutionVertexGroupEdge vertexGroupEdge : vertexGroupEdgeList) {
            builder.append(String.format("%d -> %d [label = \"%s\"]\n", vertexGroupEdge.getGroupSrcId(),
                vertexGroupEdge.getGroupTargetId(), vertexGroupEdge.getPartitioner().getPartitionType().toString()));
        }
        for (ExecutionVertexGroup vertexGroup : vertexGroupMap.values()) {
            builder.append(String
                .format(NODE_FORMAT, vertexGroup.getGroupId(), vertexGroup));
        }

        builder.append("0 [label=\"node-0\"]\n");
        builder.append(String.format(NODE_FORMAT, SCHEDULER, SCHEDULER));

        builder.append("}");
        return builder.toString();
    }

    @Override
    public String toString() {
        return executionGraph.toString();
    }
}
