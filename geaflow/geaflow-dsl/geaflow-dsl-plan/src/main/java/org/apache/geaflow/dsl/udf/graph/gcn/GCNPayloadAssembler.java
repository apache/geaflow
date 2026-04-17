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

package org.apache.geaflow.dsl.udf.graph.gcn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GCNPayloadAssembler {

    /**
     * Assemble payload for Python GCN transformer.
     *
     * <pre>
     * {
     *   "center_node_id": int|long,
     *   "sampled_nodes": List[int|long],
     *   "node_features": List[List[float]],
     *   "edge_index": List[List[int]],   // 2 x E local indices
     *   "edge_weight": List[float]
     * }
     * </pre>
     */
    public Map<String, Object> assemble(Object centerNodeId, GCNAccumulator accumulator) {
        Map<Object, Integer> vertexIndex = new LinkedHashMap<>();
        List<Object> vertexIds = new ArrayList<>();
        List<List<Double>> features = new ArrayList<>();

        for (Map.Entry<Object, List<Double>> entry : accumulator.getNodeFeatures().entrySet()) {
            vertexIndex.put(entry.getKey(), vertexIds.size());
            vertexIds.add(entry.getKey());
            features.add(new ArrayList<>(entry.getValue()));
        }

        Map<String, double[]> directedEdges = new LinkedHashMap<>();
        Map<Integer, Double> degreeMap = new LinkedHashMap<>();
        for (GCNEdgeRecord edgeRecord : accumulator.getEdgeRecords()) {
            Integer srcIndex = vertexIndex.get(edgeRecord.getSrcId());
            Integer targetIndex = vertexIndex.get(edgeRecord.getTargetId());
            if (srcIndex == null || targetIndex == null) {
                continue;
            }
            addEdge(directedEdges, degreeMap, srcIndex, targetIndex, edgeRecord.getWeight());
        }
        for (int i = 0; i < vertexIds.size(); i++) {
            addEdge(directedEdges, degreeMap, i, i, 1.0D);
        }

        List<Integer> row = new ArrayList<>();
        List<Integer> col = new ArrayList<>();
        List<Double> edgeWeight = new ArrayList<>();
        for (double[] edge : directedEdges.values()) {
            int src = (int) edge[0];
            int target = (int) edge[1];
            double rawWeight = edge[2];
            row.add(src);
            col.add(target);
            double srcDegree = degreeMap.getOrDefault(src, 1.0D);
            double targetDegree = degreeMap.getOrDefault(target, 1.0D);
            edgeWeight.add(rawWeight / Math.sqrt(srcDegree * targetDegree));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("center_node_id", centerNodeId);
        payload.put("sampled_nodes", vertexIds);
        payload.put("node_features", features);
        payload.put("edge_index", Arrays.asList(row, col));
        payload.put("edge_weight", edgeWeight);
        return payload;
    }

    private void addEdge(Map<String, double[]> directedEdges, Map<Integer, Double> degreeMap, int src,
                         int target, double weight) {
        String key = src + "->" + target;
        double[] edge = directedEdges.get(key);
        if (edge == null) {
            edge = new double[]{src, target, 0.0D};
            directedEdges.put(key, edge);
        }
        edge[2] += weight;
        degreeMap.put(src, degreeMap.getOrDefault(src, 0.0D) + weight);
    }
}
