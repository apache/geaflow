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

package org.apache.geaflow.dsl.udf.graph;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "assp", description = "built-in udga All Source Shortest Path")
public class AllSourceShortestPath implements AlgorithmUserFunction<Object, List<Object>>,
    IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, List<Object>> context;

    private EdgeDirection edgeDirection;

    @Override
    public void init(AlgorithmRuntimeContext<Object, List<Object>> context, Object[] parameters) {
        this.context = context;
        edgeDirection = EdgeDirection.OUT;
    }


    private void sendPathMessages(List<Object> path, List<RowEdge> edges) {
        if (edges == null) {
            return;
        }
        for (RowEdge edge : edges) {
            if (edgeDirection == edge.getDirect()) {
                sendPathMessage(edge.getTargetId(), path);
            }
        }
    }

    private void sendPathMessage(Object targetId, List<Object> path) {
        this.context.sendMessage(targetId, path);
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<List<Object>> messages) {
        if (context.getCurrentIterationId() == 1) {
            List<Object> path = new ArrayList<>();
            path.add(vertex.getId());
            List<RowEdge> edges = context.loadEdges(edgeDirection);
            sendPathMessages(path, edges);

            if (updatedValues.isPresent()) {
                Map<Object, List<Object>> pathMap = getPathMap(updatedValues);
                // Send paths of the current vertex to neighborhood.
                for (Entry<Object, List<Object>> paths : pathMap.entrySet()) {
                    sendPathMessages(paths.getValue(), edges);
                }
            } else {
                // Init pathMap.
                context.updateVertexValue(ObjectRow.create(new HashMap<>()));
            }
            return;
        }

        // Key is the id of src vertex.
        Map<Object, List<Object>> minPathMap = new HashMap<>();

        while (messages.hasNext()) {
            List<Object> msgPath = messages.next();

            int msgDis = getPathDist(msgPath);
            Object srcId = getSrcId(msgPath);
            if (srcId.equals(vertex.getId())) {
                continue;
            }
            if (!minPathMap.containsKey(srcId)) {
                minPathMap.put(srcId, new ArrayList<>());
            }

            int dist = getPathDist(minPathMap.get(srcId));
            // Find the min dist path.
            if (msgDis < dist) {
                minPathMap.put(srcId, msgPath);
            }
        }

        // Look up all src vertices.
        for (Entry<Object, List<Object>> entry : minPathMap.entrySet()) {
            Object srcId = entry.getKey();
            Map<Object, List<Object>> pathMap = getPathMap(updatedValues);

            if (!pathMap.containsKey(srcId)) {
                pathMap.put(srcId, new ArrayList<>());
            }

            List<Object> path = pathMap.get(srcId);
            int curVertexDis = getPathDist(path);

            List<Object> minPath = entry.getValue();
            int pathDist = getPathDist(minPath);

            // Update if minDist is less than current.
            if (pathDist < curVertexDis) {

                List<Object> newPath = new ArrayList<>(minPath);
                // Add id of current vertex to the path.
                newPath.add(vertex.getId());

                // Send path message to neighborhood.
                sendPathMessages(newPath, context.loadEdges(edgeDirection));

                // update the pathMap of current vertex.
                pathMap.put(srcId, newPath);
                context.updateVertexValue(ObjectRow.create(pathMap));
            }
        }
    }

    Map<Object, List<Object>> getPathMap(Optional<Row> value) {
        return (Map<Object, List<Object>>) value.get().getField(0, null);
    }

    int getPathDist(List<Object> path) {
        if (path.isEmpty()) {
            return Integer.MAX_VALUE;
        }
        return path.size() - 1;
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("vertex", vertex.getId());
        if (newValue.isPresent()) {
            jsonObject.put("paths", getPathMap(newValue));
        } else {
            jsonObject.put("paths", "[]");
        }

        context.take(ObjectRow.create(jsonObject.toString()));
    }

    private Object getSrcId(List<Object> path) {
        return path.get(0);
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(new TableField("res", StringType.INSTANCE, false));
    }
}
