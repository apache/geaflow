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

package org.apache.geaflow.core.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionVertexGroup implements Serializable {

    private int groupId;

    // A execution vertex group contains one or many vertex.
    private Map<Integer, ExecutionVertex> vertexMap;

    // Keep the mapping that edge id with edge.
    private Map<Integer, ExecutionEdge> edgeMap;

    // Keep the mapping that edge id with iteration edge.
    private Map<Integer, ExecutionEdge> iterationEdgeMap;

    // Keep the parent vertex group id list of current vertex group.
    private List<Integer> parentVertexGroupIds;

    // Keep the children vertex group id list of current vertex group.
    private List<Integer> childrenVertexGroupIds;

    // <vertex id, List<edge id of output edge>>, it's used to be describe the relation with out vertex group.
    private Map<Integer, List<Integer>> vertexId2OutEdgeIds;

    // <vertex id, List<edge id of input edge>>, it's used to be describe the relation with in vertex group.
    private Map<Integer, List<Integer>> vertexId2InEdgeIds;

    private CycleGroupMeta cycleGroupMeta;

    public ExecutionVertexGroup(int groupId) {
        this.groupId = groupId;
        this.vertexMap = new HashMap<>();
        this.edgeMap = new HashMap<>();
        this.iterationEdgeMap = new HashMap<>();
        this.parentVertexGroupIds = new ArrayList<>();
        this.childrenVertexGroupIds = new ArrayList<>();
        this.vertexId2OutEdgeIds = new HashMap<>();
        this.vertexId2InEdgeIds = new HashMap<>();
        this.cycleGroupMeta = new CycleGroupMeta();
    }

    public int getGroupId() {
        return groupId;
    }

    public Map<Integer, ExecutionVertex> getVertexMap() {
        return vertexMap;
    }

    public Map<Integer, ExecutionEdge> getEdgeMap() {
        return edgeMap;
    }

    public Map<Integer, ExecutionEdge> getIterationEdgeMap() {
        return this.iterationEdgeMap;
    }

    public Map<Integer, List<Integer>> getVertexId2OutEdgeIds() {
        return vertexId2OutEdgeIds;
    }

    public Map<Integer, List<Integer>> getVertexId2InEdgeIds() {
        return vertexId2InEdgeIds;
    }

    public void putVertexId2OutEdgeIds(int vertexId, List<Integer> outEdgeIds) {
        this.vertexId2OutEdgeIds.put(vertexId, outEdgeIds);
    }

    public void putVertexId2InEdgeIds(int vertexId, List<Integer> inEdgeIds) {
        this.vertexId2InEdgeIds.put(vertexId, inEdgeIds);
    }

    public List<Integer> getParentVertexGroupIds() {
        return parentVertexGroupIds;
    }

    public List<Integer> getChildrenVertexGroupIds() {
        return childrenVertexGroupIds;
    }

    public CycleGroupMeta getCycleGroupMeta() {
        return cycleGroupMeta;
    }

    /**
     * Get the tail vertex ids in the current vertex group.
     */
    public List<Integer> getTailVertexIds() {
        List<Integer> tailVertexIds = new ArrayList<>();

        vertexMap.keySet().stream().forEach(vertexId -> {
            List<Integer> outputVertexIds = new ArrayList<>();
            vertexId2OutEdgeIds.get(vertexId).stream().forEach(edgeId -> {
                if (edgeMap.get(edgeId) != null) {
                    outputVertexIds.add(edgeMap.get(edgeId).getTargetId());
                }
            });
            if (outputVertexIds.size() == 0) {
                tailVertexIds.add(vertexId);
            } else {
                boolean flag = false;
                for (int outputVertexId : outputVertexIds) {
                    if (!vertexMap.containsKey(outputVertexId)) {
                        flag = true;
                        break;
                    }
                }
                if (flag) {
                    tailVertexIds.add(vertexId);
                }
            }
        });

        return tailVertexIds;
    }

    /**
     * Get the head vertex ids in the current vertex group.
     */
    public List<Integer> getHeadVertexIds() {
        List<Integer> headVertexIds = new ArrayList<>();

        vertexMap.keySet().stream().forEach(vertexId -> {
            // 1. Vertex input edge is empty OR
            // 2. Vertex input edge's source vertex not belong to current group
            boolean hasNoInputEdge = vertexId2InEdgeIds.get(vertexId) == null || vertexId2InEdgeIds.get(vertexId).isEmpty();
            boolean isVertexSrcNotInGroup = vertexId2InEdgeIds.get(vertexId).stream()
                .anyMatch(edgeId -> !vertexMap.containsKey(edgeMap.get(edgeId).getSrcId()));
            if (hasNoInputEdge || isVertexSrcNotInGroup) {
                headVertexIds.add(vertexId);
            }
        });

        return headVertexIds;
    }

    @Override
    public String toString() {
        return "ExecutionVertexGroup{" + vertexMap
            + ",\n cycleGroupMeta=" + cycleGroupMeta
            + "\n}";
    }
}
