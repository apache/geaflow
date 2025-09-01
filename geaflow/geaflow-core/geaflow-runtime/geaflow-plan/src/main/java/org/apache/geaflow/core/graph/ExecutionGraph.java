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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionGraph implements Serializable {

    // A execution graph contains one or many vertexGroup.
    private Map<Integer, ExecutionVertexGroup> vertexGroupMap;

    // It specifies the edge map of vertex groups.
    private Map<Integer, ExecutionVertexGroupEdge> groupEdgeMap;

    // <group id, List<edge id of out group>>, it's used to be describe the relation with out vertex group.
    private Map<Integer, List<Integer>> vertexGroupOutEdgeIds;

    // <group id, List<edge id of in group>>, it's used to be describe the relation with in vertex group.
    private Map<Integer, List<Integer>> vertexGroupInEdgeIds;

    private CycleGroupMeta cycleGroupMeta;

    public ExecutionGraph() {
        this.vertexGroupMap = new HashMap<>();
        this.groupEdgeMap = new HashMap<>();
        this.vertexGroupOutEdgeIds = new HashMap<>();
        this.vertexGroupInEdgeIds = new HashMap<>();
        this.cycleGroupMeta = new CycleGroupMeta();
    }

    public Map<Integer, ExecutionVertexGroup> getVertexGroupMap() {
        return vertexGroupMap;
    }

    public void setVertexGroupMap(Map<Integer, ExecutionVertexGroup> vertexGroupMap) {
        this.vertexGroupMap = vertexGroupMap;
    }

    public Map<Integer, ExecutionVertexGroupEdge> getGroupEdgeMap() {
        return groupEdgeMap;
    }

    public void setGroupEdgeMap(Map<Integer, ExecutionVertexGroupEdge> groupEdgeMap) {
        this.groupEdgeMap = groupEdgeMap;
    }

    public Map<Integer, List<Integer>> getVertexGroupOutEdgeIds() {
        return vertexGroupOutEdgeIds;
    }

    public void setVertexGroupOutEdgeIds(Map<Integer, List<Integer>> vertexGroupOutEdgeIds) {
        this.vertexGroupOutEdgeIds = vertexGroupOutEdgeIds;
    }

    public Map<Integer, List<Integer>> getVertexGroupInEdgeIds() {
        return vertexGroupInEdgeIds;
    }

    public void setVertexGroupInEdgeIds(Map<Integer, List<Integer>> vertexGroupInEdgeIds) {
        this.vertexGroupInEdgeIds = vertexGroupInEdgeIds;
    }

    public void putVertexGroupOutEdgeIds(int groupId, List<Integer> outEdgeIds) {
        this.vertexGroupOutEdgeIds.put(groupId, outEdgeIds);
    }

    public void putVertexGroupInEdgeIds(int groupId, List<Integer> inEdgeIds) {
        this.vertexGroupInEdgeIds.put(groupId, inEdgeIds);
    }

    public CycleGroupMeta getCycleGroupMeta() {
        return cycleGroupMeta;
    }

    @Override
    public String toString() {
        return "ExecutionGraph{" + "vertexGroupMap=" + vertexGroupMap
            + ",\n vertexGroupOutEdgeIds=" + vertexGroupOutEdgeIds
            + ",\n vertexGroupInEdgeIds=" + vertexGroupInEdgeIds
            + ",\n cycleGroupMeta=" + cycleGroupMeta + "\n}";
    }
}
