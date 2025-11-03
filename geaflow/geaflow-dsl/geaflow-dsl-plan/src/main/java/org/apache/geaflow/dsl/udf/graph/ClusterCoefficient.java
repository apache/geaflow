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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.IntegerType;
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
import org.apache.geaflow.model.graph.edge.EdgeDirection;

/**
 * ClusterCoefficient Algorithm Implementation.
 * 
 * <p>The clustering coefficient of a node measures how close its neighbors are to being 
 * a complete graph (clique). It is calculated as the ratio of the number of edges between 
 * neighbors to the maximum possible number of edges between them.
 * 
 * <p>Formula: C(v) = 2 * T(v) / (k(v) * (k(v) - 1))
 * where:
 * - T(v) is the number of triangles through node v
 * - k(v) is the degree of node v
 * 
 * <p>The algorithm consists of 3 iteration phases:
 * 1. First iteration: Each node sends its neighbor list to all neighbors
 * 2. Second iteration: Each node receives neighbor lists and calculates connections
 * 3. Third iteration: Output final clustering coefficient results
 * 
 * <p>Supports parameters:
 * - vertexType (optional): Filter nodes by vertex type
 * - minDegree (optional): Minimum degree threshold (default: 2)
 */
@Description(name = "cluster_coefficient", description = "built-in udga for Cluster Coefficient.")
public class ClusterCoefficient implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    private static final int MAX_ITERATION = 3;
    
    // Parameters
    private String vertexType = null;
    private int minDegree = 2;
    
    // Exclude set for nodes that don't match the vertex type filter
    private final Set<Object> excludeSet = Sets.newHashSet();

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        
        // Validate parameter count
        if (params.length > 2) {
            throw new IllegalArgumentException(
                "Maximum parameter limit exceeded. Expected: [vertexType], [minDegree]");
        }
        
        // Parse parameters based on type
        // If first param is String, it's vertexType; if it's Integer/Long, it's minDegree
        if (params.length >= 1 && params[0] != null) {
            if (params[0] instanceof String) {
                // First param is vertexType
                vertexType = (String) params[0];
                
                // Second param (if exists) is minDegree
                if (params.length >= 2 && params[1] != null) {
                    if (!(params[1] instanceof Integer || params[1] instanceof Long)) {
                        throw new IllegalArgumentException(
                            "Minimum degree parameter should be integer.");
                    }
                    minDegree = params[1] instanceof Integer 
                        ? (Integer) params[1] 
                        : ((Long) params[1]).intValue();
                }
            } else if (params[0] instanceof Integer || params[0] instanceof Long) {
                // First param is minDegree (no vertexType filter)
                vertexType = null;
                minDegree = params[0] instanceof Integer 
                    ? (Integer) params[0] 
                    : ((Long) params[0]).intValue();
            } else {
                throw new IllegalArgumentException(
                    "Parameter should be either string (vertexType) or integer (minDegree).");
            }
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        updatedValues.ifPresent(vertex::setValue);
        
        Object vertexId = vertex.getId();
        long currentIteration = context.getCurrentIterationId();

        if (currentIteration == 1L) {
            // First iteration: Check vertex type filter and send neighbor lists
            if (Objects.nonNull(vertexType) && !vertexType.equals(vertex.getLabel())) {
                excludeSet.add(vertexId);
                // Send heartbeat to keep vertex alive
                context.sendMessage(vertexId, ObjectRow.create(-1));
                return;
            }

            // Load all neighbors (both directions for undirected graph)
            List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
            
            // Get unique neighbor IDs
            Set<Object> neighborSet = Sets.newHashSet();
            for (RowEdge edge : edges) {
                Object neighborId = edge.getTargetId();
                if (!excludeSet.contains(neighborId)) {
                    neighborSet.add(neighborId);
                }
            }
            
            int degree = neighborSet.size();
            
            // For nodes with degree < minDegree, clustering coefficient is 0
            if (degree < minDegree) {
                // Store degree and triangle count = 0
                context.updateVertexValue(ObjectRow.create(degree, 0));
                context.sendMessage(vertexId, ObjectRow.create(-1));
                return;
            }
            
            // Build neighbor list message: [degree, neighbor1, neighbor2, ...]
            List<Object> neighborInfo = Lists.newArrayList();
            neighborInfo.add(degree);
            neighborInfo.addAll(neighborSet);
            
            ObjectRow neighborListMsg = ObjectRow.create(neighborInfo.toArray());
            
            // Send neighbor list to all neighbors
            for (Object neighborId : neighborSet) {
                context.sendMessage(neighborId, neighborListMsg);
            }
            
            // Store neighbor list in vertex value for next iteration
            context.updateVertexValue(neighborListMsg);
            
            // Send heartbeat to self
            context.sendMessage(vertexId, ObjectRow.create(-1));
            
        } else if (currentIteration == 2L) {
            // Second iteration: Calculate connections between neighbors
            if (excludeSet.contains(vertexId)) {
                context.sendMessage(vertexId, ObjectRow.create(-1));
                return;
            }
            
            Row vertexValue = vertex.getValue();
            if (vertexValue == null) {
                context.sendMessage(vertexId, ObjectRow.create(-1));
                return;
            }
            
            int degree = (int) vertexValue.getField(0, IntegerType.INSTANCE);
            
            // For nodes with degree < minDegree, skip calculation
            if (degree < minDegree) {
                context.sendMessage(vertexId, ObjectRow.create(-1));
                return;
            }
            
            // Get this vertex's neighbor set
            Set<Object> myNeighbors = row2Set(vertexValue);
            
            // Count triangles by checking common neighbors
            int triangleCount = 0;
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                
                // Skip heartbeat messages
                int msgDegree = (int) msg.getField(0, IntegerType.INSTANCE);
                if (msgDegree < 0) {
                    continue;
                }
                
                // Get neighbor's neighbor set
                Set<Object> neighborNeighbors = row2Set(msg);
                
                // Count common neighbors (forming triangles)
                neighborNeighbors.retainAll(myNeighbors);
                triangleCount += neighborNeighbors.size();
            }
            
            // Store degree and triangle count for final calculation
            context.updateVertexValue(ObjectRow.create(degree, triangleCount));
            context.sendMessage(vertexId, ObjectRow.create(-1));
            
        } else if (currentIteration == 3L) {
            // Third iteration: Calculate and output clustering coefficient
            if (excludeSet.contains(vertexId)) {
                return;
            }
            
            Row vertexValue = vertex.getValue();
            if (vertexValue == null) {
                return;
            }
            
            int degree = (int) vertexValue.getField(0, IntegerType.INSTANCE);
            int triangleCount = (int) vertexValue.getField(1, IntegerType.INSTANCE);
            
            // Calculate clustering coefficient
            double coefficient;
            if (degree < minDegree) {
                coefficient = 0.0;
            } else {
                // C(v) = 2 * T(v) / (k(v) * (k(v) - 1))
                // Note: triangleCount is already counting edges, so we divide by 2
                double actualTriangles = triangleCount / 2.0;
                double maxPossibleEdges = degree * (degree - 1.0);
                coefficient = maxPossibleEdges > 0 
                    ? (2.0 * actualTriangles) / maxPossibleEdges 
                    : 0.0;
            }
            
            context.take(ObjectRow.create(vertexId, coefficient));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // No action needed in finish
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vid", graphSchema.getIdType(), false),
            new TableField("coefficient", DoubleType.INSTANCE, false)
        );
    }

    /**
     * Convert Row to Set of neighbor IDs.
     * Row format: [degree, neighbor1, neighbor2, ...]
     */
    private Set<Object> row2Set(Row row) {
        int degree = (int) row.getField(0, IntegerType.INSTANCE);
        Set<Object> neighborSet = Sets.newHashSet();
        for (int i = 1; i <= degree; i++) {
            Object neighborId = row.getField(i, context.getGraphSchema().getIdType());
            if (!excludeSet.contains(neighborId)) {
                neighborSet.add(neighborId);
            }
        }
        return neighborSet;
    }
}
