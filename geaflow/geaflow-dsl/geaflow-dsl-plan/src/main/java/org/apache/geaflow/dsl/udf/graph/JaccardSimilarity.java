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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

@Description(name = "jaccard_similarity", description = "built-in udga for Jaccard Similarity")
public class JaccardSimilarity implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    // tuple to store params
    private Tuple<Object, Object> vertices;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;

        if (params.length != 2) {
            throw new IllegalArgumentException("Only support two arguments, usage: jaccard_similarity(id_a, id_b)");
        }
        this.vertices = new Tuple<>(
            TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType()),
            TypeCastUtil.cast(params[1], context.getGraphSchema().getIdType())
        );
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // First iteration: vertices A and B compute their neighbor counts
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                Object sourceId = vertex.getId();
                
                // Calculate unique neighbors count (de-duplicate and exclude self-loops)
                Set<Object> uniqueNeighbors = new HashSet<>();
                for (RowEdge edge : edges) {
                    Object targetId = edge.getTargetId();
                    // Exclude self-loops: only add if targetId != sourceId
                    if (!sourceId.equals(targetId)) {
                        uniqueNeighbors.add(targetId);
                    }
                }
                
                // Calculate neighbor count for this vertex
                long neighborCount = uniqueNeighbors.size();
                
                // Send messages to all unique neighbors
                // Message format: [sourceId, neighborCount, messageType]
                // messageType = 0: neighbor inquiry, messageType = 1: count from target vertex
                for (Object neighbor : uniqueNeighbors) {
                    context.sendMessage(neighbor, ObjectRow.create(sourceId, neighborCount, 0L));
                }
                
                // Send neighbor count to the other target vertex (A ↔ B exchange)
                // Message format: [vertexId, neighborCount, messageType]
                // messageType = 1: this is a count message from target vertex B
                if (vertices.f0.equals(sourceId) && !vertices.f0.equals(vertices.f1)) {
                    context.sendMessage(vertices.f1, ObjectRow.create(sourceId, neighborCount, 1L));
                } else if (vertices.f1.equals(sourceId) && !vertices.f0.equals(vertices.f1)) {
                    context.sendMessage(vertices.f0, ObjectRow.create(sourceId, neighborCount, 1L));
                }
            }
        } else if (context.getCurrentIterationId() == 2L) {
            // Second iteration: calculate Jaccard similarity
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                // Extract neighbor counts and count common neighbors
                long neighborCountA = 0;
                long neighborCountB = 0;
                long localCommonNeighborCount = 0;
                
                while (messages.hasNext()) {
                    ObjectRow message = messages.next();
                    Object senderId = message.getField(0, context.getGraphSchema().getIdType());
                    long count = (Long) message.getField(1, org.apache.geaflow.common.type.primitive.LongType.INSTANCE);
                    long messageType = (Long) message.getField(2, org.apache.geaflow.common.type.primitive.LongType.INSTANCE);
                    
                    // messageType = 1: neighbor count from the other target vertex (A or B)
                    // messageType = 0: confirmation from common neighbor
                    if (messageType == 1L) {
                        // This is a count message from target vertex
                        if (vertices.f0.equals(senderId)) {
                            neighborCountA = count;
                        } else if (vertices.f1.equals(senderId)) {
                            neighborCountB = count;
                        }
                    } else {
                        // This is a confirmation from a common neighbor
                        localCommonNeighborCount++;
                    }
                }
                
                // Calculate and output the Jaccard coefficient only from vertex A
                if (vertices.f0.equals(vertex.getId())) {
                    // If neighborCountA is 0, calculate it from edges
                    if (neighborCountA == 0) {
                        Object sourceId = vertex.getId();
                        List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                        Set<Object> neighbors = new HashSet<>();
                        for (RowEdge edge : edges) {
                            Object targetId = edge.getTargetId();
                            if (!sourceId.equals(targetId)) {
                                neighbors.add(targetId);
                            }
                        }
                        neighborCountA = neighbors.size();
                    }
                    
                    // Calculate Jaccard coefficient: |A ∩ B| / |A ∪ B|
                    long intersection = localCommonNeighborCount;
                    long union = neighborCountA + neighborCountB - intersection;
                    double jaccardCoefficient = union == 0 ? 0.0 : (double) intersection / union;
                    
                    // Output the result
                    context.take(ObjectRow.create(vertices.f0, vertices.f1, jaccardCoefficient));
                }
            } else {
                // For non-A, non-B vertices: check if they received messages from both A and B
                boolean receivedFromA = false;
                boolean receivedFromB = false;
                
                while (messages.hasNext()) {
                    ObjectRow message = messages.next();
                    Object senderId = message.getField(0, context.getGraphSchema().getIdType());
                    long messageType = (Long) message.getField(2, org.apache.geaflow.common.type.primitive.LongType.INSTANCE);
                    
                    // Only count messages with type 0 (neighbor inquiry)
                    if (messageType == 0L) {
                        if (vertices.f0.equals(senderId)) {
                            receivedFromA = true;
                        }
                        if (vertices.f1.equals(senderId)) {
                            receivedFromB = true;
                        }
                    }
                }
                
                // If this vertex received messages from both A and B, it's a common neighbor
                // Send confirmation to vertex A with format [vertexId, 1, 0]
                if (receivedFromA && receivedFromB) {
                    context.sendMessage(vertices.f0, ObjectRow.create(vertex.getId(), 1L, 0L));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // No additional finish processing needed
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vertex_a", graphSchema.getIdType(), false),
            new TableField("vertex_b", graphSchema.getIdType(), false),
            new TableField("jaccard_coefficient", DoubleType.INSTANCE, false)
        );
    }
}