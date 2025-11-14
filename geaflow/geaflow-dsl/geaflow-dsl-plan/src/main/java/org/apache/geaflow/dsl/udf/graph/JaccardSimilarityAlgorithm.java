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

import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Description(name = "jaccard_similarity", description = "built-in udga for Jaccard Similarity")
public class JaccardSimilarityAlgorithm implements AlgorithmUserFunction<Object, Object> {

    private static final String PREFIX = "EDGE_COUNT_";

    private AlgorithmRuntimeContext<Object, Object> context;

    private Tuple<Object, Object> vertices;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] params) {
        this.context = context;

        if (params.length != 2) {
            throw new IllegalArgumentException("Only support two arguments, usage: common_neighbors(id_a, id_b)");
        }
        this.vertices = new Tuple<>(TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType()),
                TypeCastUtil.cast(params[1], context.getGraphSchema().getIdType()));
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        // similarity = |A ∩ B| / |A ∪ B| = |A ∩ B| / ( |A| + |B| - |A ∩ B| )
        // so we can calculate |A ∩ B| , |A| and |B| to calculate similarity

        Object vertexId = vertex.getId();
        long currentIteration = context.getCurrentIterationId();

        switch ((int) currentIteration) {
            case 1:
                processFirstIteration(vertexId);
                break;
            case 2:
                processSecondIteration(vertexId, messages);
                break;
            case 3:
                processThirdIteration(vertexId, messages);
                break;
            default:
                // Do nothing for other iterations
                break;
        }
    }

    private void processFirstIteration(Object vertexId) {
        // send message to neighbors if they are vertices in params
        if (isTargetVertex(vertexId)) {
            List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
            sendMessageToNeighbors(edges, vertexId);
        }
    }

    private void processSecondIteration(Object vertexId, Iterator<Object> messages) {
        if (vertices.f0.equals(vertexId)) {
            List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
            long neighborCount = calculateDeduplicatedSize(edges, vertexId);
            // send neighbor count of A to B
            context.sendMessage(vertices.f1, PREFIX + neighborCount);
        }

        // collect messages from both vertices to find common neighbors
        boolean receivedFromA = false;
        boolean receivedFromB = false;

        while (messages.hasNext()) {
            Object message = messages.next();

            if (vertices.f0.equals(message)) {
                receivedFromA = true;
            } else if (vertices.f1.equals(message)) {
                receivedFromB = true;
            }

            // Forward common neighbors to vertex B for final calculation
            if (receivedFromA && receivedFromB) {
                context.sendMessage(vertices.f1, message);
                break;
            }
        }
    }

    private void processThirdIteration(Object vertexId, Iterator<Object> messages) {
        if (!vertices.f1.equals(vertexId)) {
            return;
        }

        List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
        long vertexBNeighborCount = calculateDeduplicatedSize(edges, vertexId);

        long vertexANeighborCount = 0;
        Set<Object> commonNeighbors = new HashSet<>();

        while (messages.hasNext()) {
            Object message = messages.next();
            if (message instanceof String && ((String) message).startsWith(PREFIX)) {
                // Received neighbor count from vertex A
                vertexANeighborCount = Long.parseLong(((String) message).substring(PREFIX.length()));
            } else {
                // Received common neighbor
                commonNeighbors.add(message);
            }
        }

        long commonNeighborCount = commonNeighbors.size();
        long unionNeighborCount = vertexANeighborCount + vertexBNeighborCount - commonNeighborCount;

        double similarity = calculateJaccardSimilarity(commonNeighborCount, unionNeighborCount);
        context.take(ObjectRow.create(vertices.f0, vertices.f1, similarity));
    }

    private boolean isTargetVertex(Object vertexId) {
        return vertices.f0.equals(vertexId) || vertices.f1.equals(vertexId);
    }

    private double calculateJaccardSimilarity(long commonCount, long unionCount) {
        if (unionCount == 0) {
            return commonCount == 0 ? 1.0 : 0.0;
        }
        return (double) commonCount / unionCount;
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(new TableField("vertex_a", graphSchema.getIdType(), false),
                new TableField("vertex_b", graphSchema.getIdType(), false),
                new TableField("jaccard_coefficient", DoubleType.INSTANCE, false));
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Object message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }

    private long calculateDeduplicatedSize(List<RowEdge> edges, Object vertexId) {
        Set<Object> set = new HashSet<>();
        for (RowEdge edge : edges) {
            Object srcId = edge.getSrcId();
            Object targetId = edge.getTargetId();
            if (vertexId.equals(srcId)) {
                set.add(targetId);
            } else {
                set.add(srcId);
            }
        }
        return set.size();
    }
}