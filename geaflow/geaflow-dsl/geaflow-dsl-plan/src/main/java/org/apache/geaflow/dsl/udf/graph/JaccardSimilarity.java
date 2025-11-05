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
import org.apache.geaflow.common.type.primitive.LongType;
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

    // Store neighbor counts for vertex A and B
    private long neighborsA = 0;
    private long neighborsB = 0;

    // Store common neighbors
    private final Set<Object> commonNeighbors = new HashSet<>();

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
            // First iteration: send messages to neighbors if they are vertices in params
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                Object sourceId = vertex.getId();
                
                // Store neighbor count for Jaccard calculation
                if (vertices.f0.equals(sourceId)) {
                    neighborsA = edges.size();
                } else if (vertices.f1.equals(sourceId)) {
                    neighborsB = edges.size();
                }
                
                // Send message to all neighbors with source ID and neighbor count
                ObjectRow message = ObjectRow.create(sourceId, (long) edges.size());
                for (RowEdge edge : edges) {
                    context.sendMessage(edge.getTargetId(), message);
                }
                
                // Also send message to the other vertex to ensure both know about each other
                if (vertices.f0.equals(sourceId) && !vertices.f0.equals(vertices.f1)) {
                    context.sendMessage(vertices.f1, message);
                } else if (vertices.f1.equals(sourceId) && !vertices.f0.equals(vertices.f1)) {
                    context.sendMessage(vertices.f0, message);
                }
            }
        } else if (context.getCurrentIterationId() == 2L) {
            // Second iteration: calculate Jaccard similarity
            // Check if this vertex is one of the target vertices (A or B)
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                // Collect messages to determine common neighbors
                Set<Object> sendersA = new HashSet<>();
                Set<Object> sendersB = new HashSet<>();
                
                while (messages.hasNext()) {
                    ObjectRow message = messages.next();
                    Object senderId = message.getField(0, context.getGraphSchema().getIdType());
                    long neighborCount = (Long) message.getField(1, LongType.INSTANCE);
                    
                    // Track which vertex sent the message
                    if (vertices.f0.equals(senderId)) {
                        sendersA.add(vertex.getId());
                    }
                    if (vertices.f1.equals(senderId)) {
                        sendersB.add(vertex.getId());
                    }
                }
                
                // If this vertex received messages from both A and B, it's a common neighbor
                if (sendersA.contains(vertex.getId()) && sendersB.contains(vertex.getId())) {
                    commonNeighbors.add(vertex.getId());
                }
                
                // When we reach vertex A or B, calculate and output the Jaccard coefficient
                if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                    // Calculate Jaccard coefficient: |A ∩ B| / |A ∪ B|
                    long intersection = commonNeighbors.size();
                    long union = neighborsA + neighborsB - intersection;
                    
                    // Avoid division by zero
                    double jaccardCoefficient = union == 0 ? 0.0 : (double) intersection / union;
                    
                    // Output the result only once (from one of the vertices)
                    if (vertices.f0.equals(vertex.getId())) {
                        context.take(ObjectRow.create(vertices.f0, vertices.f1, jaccardCoefficient));
                    }
                }
            } else {
                // For common neighbors, collect messages and send back information
                while (messages.hasNext()) {
                    ObjectRow message = messages.next();
                    Object senderId = message.getField(0, context.getGraphSchema().getIdType());
                    
                    // Send back acknowledgment to the sender
                    ObjectRow ackMessage = ObjectRow.create(vertex.getId());
                    context.sendMessage(senderId, ackMessage);
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