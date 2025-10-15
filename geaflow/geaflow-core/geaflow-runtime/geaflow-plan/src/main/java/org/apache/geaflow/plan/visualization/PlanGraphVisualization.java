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

package org.apache.geaflow.plan.visualization;

import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;


public class PlanGraphVisualization {

    public static final String SCHEDULER = "scheduler";
    private static final String NODE_FORMAT = "%s [label=\"%s\"]\n";
    private static final String EMPTY_GRAPH = new StringBuilder("digraph G {\n")
        .append("0 [label=\"node-0\"]\n")
        .append(String.format(NODE_FORMAT, SCHEDULER, SCHEDULER))
        .append("}")
        .toString();

    private PipelineGraph plan;
    private List<PipelineEdge> pipelineEdgeList;
    private Map<Integer, PipelineVertex> pipelineVertexMap;

    public PlanGraphVisualization(PipelineGraph plan) {
        this.plan = plan;
        this.pipelineEdgeList = new ArrayList<>(plan.getPipelineEdgeList());
        this.pipelineVertexMap = plan.getVertexMap();
    }

    public String getGraphviz() {
        // Determine whether an empty PipelineGraph that has already executed a batch job is passed in.
        if (pipelineEdgeList.size() == 0 && pipelineVertexMap.size() == 0) {
            return EMPTY_GRAPH;
        }
        Collections.sort(pipelineEdgeList, (o1, o2) -> {
            int i = Integer.compare(o1.getSrcId(), o2.getSrcId());
            if (i == 0) {
                return Integer.compare(o1.getTargetId(), o2.getTargetId());
            } else {
                return i;
            }
        });

        StringBuilder builder = new StringBuilder("digraph G {\n");
        for (PipelineEdge edge : pipelineEdgeList) {
            builder.append(String
                .format("%d -> %d [label = \"%s\"]\n", edge.getSrcId(), edge.getTargetId(),
                    edge.getPartitionType().toString()));
        }
        for (PipelineVertex vertex : pipelineVertexMap.values()) {
            builder.append(String
                .format(NODE_FORMAT, vertex.getVertexId(), vertex.getVertexString()));
        }

        builder.append("0 [label=\"node-0\"]\n");
        builder.append(String.format(NODE_FORMAT, SCHEDULER, SCHEDULER));

        builder.append("}");
        return builder.toString();
    }

    public String getNodeInfo() {
        Map<String, GeaFlowNodeInfo> id2Info = new HashMap<>();
        for (PipelineVertex vertex : pipelineVertexMap.values()) {
            GeaFlowNodeInfo node = new GeaFlowNodeInfo(vertex.getVertexId(), vertex.getType().name(),
                vertex.getOperator());
            id2Info.put(vertex.getVertexName(), node);

        }
        return JSON.toJSONString(id2Info);
    }

    @Override
    public String toString() {
        return plan.toString();
    }
}
