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

package org.apache.geaflow.console.core.model.release;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.service.integration.engine.JsonPlan;
import org.apache.geaflow.console.common.service.integration.engine.Predecessor;
import org.apache.geaflow.console.common.service.integration.engine.Vertex;
import org.apache.geaflow.console.core.model.release.JobPlan.PlanEdge;
import org.apache.geaflow.console.core.model.release.JobPlan.PlanVertex;

@Getter
@Setter
public class JobPlanBuilder {

    public static JobPlan build(JsonPlan jsonPlan) {
        JobPlanContext jobPlanContext = new JobPlanContext();
        return jobPlanContext.build(jsonPlan);
    }

    public static void setParallelisms(JobPlan jobPlan, Map<String, Integer> map) {
        for (PlanVertex vertex : jobPlan.getVertices().values()) {
            String key = vertex.getKey();
            if (map.containsKey(key)) {
                int parallelism = map.get(key);
                // set inner parallelism recursively
                setInnerParallelism(vertex, parallelism, map);
            }
        }
    }

    private static void setInnerParallelism(PlanVertex vertex, int parallelism, Map<String, Integer> map) {
        if (vertex.getInnerPlan() == null) {
            return;
        }

        for (PlanVertex v : vertex.getInnerPlan().getVertices().values()) {
            map.put(v.getKey(), parallelism);
            setInnerParallelism(v, parallelism, map);
        }
    }

    public static Map<String, Integer> getParallelismMap(JobPlan jobPlan) {
        Map<String, Integer> map = new HashMap<>();
        getInnerParallelism(jobPlan, map);
        return map;
    }

    private static void getInnerParallelism(JobPlan jobPlan, Map<String, Integer> map) {
        for (PlanVertex v : jobPlan.getVertices().values()) {
            map.put(v.getKey(), v.getParallelism());
            if (v.getInnerPlan() != null) {
                getInnerParallelism(v.getInnerPlan(), map);
            }
        }
    }

    private static class JobPlanContext {

        private int level = 0;
        private Map<String, String> keyMap = new HashMap<>();

        private JobPlan build(JsonPlan jsonPlan) {

            JobPlan jobPlan = new JobPlan();
            Map<String, Vertex> vertexMap = jsonPlan.getVertices();
            for (Vertex vertex : vertexMap.values()) {
                level++;
                keyMap.put(vertex.getId(), getVertexKey(vertex));
                PlanVertex planVertex = getPlanVertex(vertex);
                jobPlan.getVertices().put(planVertex.getKey(), planVertex);
                level--;
            }
            // set edges after getting the mapping of vertex ids and keys
            for (Vertex vertex : vertexMap.values()) {
                setEdgeMap(jobPlan.getEdgeMap(), vertex);
            }
            return jobPlan;
        }

        private void setEdgeMap(Map<String, List<PlanEdge>> edgeMap, Vertex vertex) {
            List<Predecessor> parents = vertex.getParents();
            for (Predecessor parent : parents) {
                String sourceVertexKey = keyMap.get(parent.getId());
                String targetVertexKey = keyMap.get(vertex.getId());
                edgeMap.putIfAbsent(sourceVertexKey, new ArrayList<>());

                PlanEdge planEdge = new PlanEdge(sourceVertexKey, targetVertexKey, parent.getPartitionType());
                edgeMap.get(sourceVertexKey).add(planEdge);
            }
        }

        private PlanVertex getPlanVertex(Vertex vertex) {
            PlanVertex planVertex = new PlanVertex();
            planVertex.setKey(keyMap.get(vertex.getId()));
            planVertex.setParallelism(vertex.getParallelism());
            // set innerPlan recursively
            if (vertex.getInnerPlan() != null) {
                JobPlan jobPlan = build(vertex.getInnerPlan());
                planVertex.setInnerPlan(jobPlan);
            }

            return planVertex;
        }

        private String getVertexKey(Vertex vertex) {
            return level == 1 ? vertex.getVertexType() + "-" + vertex.getId() : vertex.getOperatorName();
        }
    }


}
