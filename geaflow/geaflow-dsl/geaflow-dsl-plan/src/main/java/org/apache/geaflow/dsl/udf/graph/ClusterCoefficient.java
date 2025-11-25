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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
 * - samplingThreshold + sampleSize (optional): Minimum sampling threshold and sampling Size(default: 0)
 */
@Description(name = "cluster_coefficient", description = "built-in udga for Cluster Coefficient.")
public class ClusterCoefficient implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    // Parameters
    private String vertexType = null;
    private int minDegree = 2;
    private int samplingThreshold = 0;
    private int sampleSize = 0;

    // Exclude set for nodes that don't match the vertex type filter
    private final Set<Object> excludeSet = Sets.newHashSet();

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        
        // Validate parameter count
        if (params.length > 4) {
            throw new IllegalArgumentException(
                "Maximum parameter limit exceeded. Expected: [vertexType], [minDegree], [samplingThreshold], [sampleSize]");
        }

        // Validate parameter not null
        for (Object param : params) {
            if (param == null) {
                throw new IllegalArgumentException("Parameter should not be null.");
            }
        }

        // Parse parameters
        parseParameters(params);
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

            // Determine the target list for transmission (whether to sample depends on the threshold and sample size).
            if (degree > samplingThreshold && sampleSize > 0) {
                neighborSet = sampleNeighbors(neighborSet);
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

    /**
     * sample some neighbors
     * @param neighbors origin neighbors
     * @return sampled neighbors
     */
    private Set<Object> sampleNeighbors(Set<Object> neighbors) {
        // Strategy selection threshold:
        // If only a very small portion needs to be sampled (e.g., less than 5%), use the index randomization method.
        // Avoid copying the entire huge list
        if (sampleSize < neighbors.size() * 0.05) {
            return pickRandomIndices(neighbors, neighbors.size());
        }

        // Otherwise, use partial shuffling.
        return partialShuffle(neighbors, neighbors.size());
    }

    /**
     * Auxiliary method: Sampling by random index (to save memory)
     * @param neighbors origin neighbors
     * @param totalSize origin neighbor's size
     * @return sampled neighbors
     */
    private Set<Object> pickRandomIndices(Set<Object> neighbors, int totalSize) {
        // Use List to index the element
        List<Object> neighborList = new ArrayList<>(neighbors);
        Set<Object> result = new HashSet<>(sampleSize);
        // Use Set to ensure that indices are not repeated.
        Set<Integer> selectedIndices = new HashSet<>(sampleSize);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (selectedIndices.size() < sampleSize) {
            int idx = rnd.nextInt(totalSize);
            // If add returns true, it means it's a new index.
            if (selectedIndices.add(idx)) {
                result.add(neighborList.get(idx));
            }
        }
        return result;
    }

    /**
     * Auxiliary method: Partial shuffling
     * @param neighbors origin neighbors
     * @param totalSize origin neighbor's size
     * @return sampled neighbors
     */
    private Set<Object> partialShuffle(Set<Object> neighbors, int totalSize) {
        List<Object> copy = new ArrayList<>(neighbors);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < sampleSize; i++) {
            Collections.swap(copy, i, i + rnd.nextInt(totalSize - i));
        }
        return new HashSet<>(copy.subList(0, sampleSize));
    }

    /**
     * Parse parameters from params
     * @param params params
     */
    private void parseParameters(Object[] params) {
        // If params.length == 1, params are [vertexType] or [minDegree]
        if (params.length == 1) {
            if (params[0] instanceof String) {
                vertexType = (String) params[0];
            } else {
                minDegree = (Integer) params[0];
            }
            return;
        }
        // If params.length == 2, params is [vertexType, minDegree] or [samplingThreshold, sampleSize]
        if (params.length == 2) {
            if (params[0] instanceof String) {
                vertexType = (String) params[0];
                minDegree = (Integer) params[1];
            } else {
                samplingThreshold = (Integer) params[0];
                sampleSize = (Integer) params[1];
            }
            return;
        }
        // If params.length == 3, params is [vertexType, samplingThreshold, sampleSize] or [minDegree, samplingThreshold, sampleSize]
        if (params.length == 3) {
            if (params[0] instanceof String) {
                vertexType = (String) params[0];
            } else {
                minDegree = (Integer) params[0];
            }
            samplingThreshold = (Integer) params[1];
            sampleSize = (Integer) params[2];
            return;
        }
        // If params.length == 4, params is [vertexType, minDegree, samplingThreshold, sampleSize]
        if (params.length == 4) {
            vertexType = (String) params[0];
            minDegree = (Integer) params[1];
            samplingThreshold = (Integer) params[2];
            sampleSize = (Integer) params[3];
        }
    }
}
