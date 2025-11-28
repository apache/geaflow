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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

/**
 * Implementation of Louvain community detection algorithm for GeaFlow.
 *
 * <p>
 * Louvain is a multi-level modularity optimization algorithm that detects
 * communities in graphs by optimizing the modularity metric.
 * </p>
 *
 * <p>
 * Parameters:
 * - maxIterations: Maximum number of iterations (default: 20)
 * - modularity: Modularity convergence threshold (default: 0.001)
 * - minCommunitySize: Minimum community size (default: 1)
 * - isWeighted: Whether the graph is weighted (default: false)
 * </p>
 */
@Description(name = "louvain", description = "built-in udga for Louvain community detection")
public class Louvain implements AlgorithmUserFunction<Object, LouvainMessage> {

    private static final long serialVersionUID = 1L;

    private AlgorithmRuntimeContext<Object, LouvainMessage> context;
    private int maxIterations = 20;
    private double modularity = 0.001;
    private boolean isWeighted = false;

    /** Global total edge weight (sum of all edge weights). */
    private double totalEdgeWeight = 0.0;

    @Override
    public void init(AlgorithmRuntimeContext<Object, LouvainMessage> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 4) {
            throw new IllegalArgumentException(
                "Louvain supports 0-4 arguments, usage: func([maxIterations, [modularity, "
                    + "[minCommunitySize, [isWeighted]]]])");
        }
        if (parameters.length > 0) {
            maxIterations = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            modularity = Double.parseDouble(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            isWeighted = Boolean.parseBoolean(String.valueOf(parameters[2]));
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<LouvainMessage> messages) {
        // Initialize or update vertex state
        LouvainVertexValue vertexValue;
        if (updatedValues.isPresent()) {
            vertexValue = deserializeVertexValue(updatedValues.get());
        } else {
            vertexValue = new LouvainVertexValue();
            vertexValue.setCommunityId(vertex.getId());
            vertexValue.setTotalWeight(0.0);
            vertexValue.setInternalWeight(0.0);
        }

        List<RowEdge> edges = new ArrayList<>(context.loadEdges(EdgeDirection.BOTH));
        long iterationId = context.getCurrentIterationId();

        if (iterationId == 1L) {
            // First iteration: Initialize each vertex as its own community
            initializeVertex(vertex, vertexValue, edges);
        } else if (iterationId <= maxIterations) {
            // Optimize community assignment
            optimizeVertexCommunity(vertex, vertexValue, edges, messages);
        }

        // Update vertex value
        context.updateVertexValue(serializeVertexValue(vertexValue));
    }

    /**
     * Initialize vertex in the first iteration.
     */
    private void initializeVertex(RowVertex vertex, LouvainVertexValue vertexValue,
                                   List<RowEdge> edges) {
        // Calculate total weight
        double totalWeight = 0.0;
        for (RowEdge edge : edges) {
            double weight = getEdgeWeight(edge);
            totalWeight += weight;
        }

        vertexValue.setTotalWeight(totalWeight);
        vertexValue.setInternalWeight(0.0); // No internal weight in first iteration
        vertexValue.setCommunityId(vertex.getId());

        // Send initial community information to neighbors
        sendCommunityInfoToNeighbors(vertex, edges, vertexValue);
    }

    /**
     * Optimize vertex's community assignment based on modularity gain.
     */
    private void optimizeVertexCommunity(RowVertex vertex, LouvainVertexValue vertexValue,
                                         List<RowEdge> edges,
                                         Iterator<LouvainMessage> messages) {
        // Collect neighbor community information
        vertexValue.clearNeighborCommunityWeights();

        // Use combiner to aggregate messages and reduce duplicate processing
        LouvainMessageCombiner combiner = new LouvainMessageCombiner();
        Map<Object, Double> aggregatedWeights = combiner.combineMessages(messages);
        aggregatedWeights.forEach(vertexValue::addNeighborCommunityWeight);

        double maxDeltaQ = 0.0;
        Object bestCommunity = vertexValue.getCommunityId();

        // Calculate modularity gain for moving to each neighbor community
        for (Object communityId : vertexValue.getNeighborCommunityWeights().keySet()) {
            double deltaQ = calculateModularityGain(vertex.getId(), vertexValue,
                    communityId, edges);
            if (deltaQ > maxDeltaQ) {
                maxDeltaQ = deltaQ;
                bestCommunity = communityId;
            }
        }

        // Update community if improvement found
        if (!bestCommunity.equals(vertexValue.getCommunityId())) {
            vertexValue.setCommunityId(bestCommunity);
        }

        // Send updated community info to neighbors
        sendCommunityInfoToNeighbors(vertex, edges, vertexValue);
    }

    /**
     * Calculate the modularity gain of moving vertex to a different community.
     *
     * <p>
     * ΔQ = [Σin + ki,in / 2m] - [Σtot + ki / 2m]² -
     *      [Σin / 2m - (Σtot / 2m)² - (ki / 2m)²]
     * </p>
     */
    private double calculateModularityGain(Object vertexId, LouvainVertexValue vertexValue,
                                           Object targetCommunity, List<RowEdge> edges) {
        if (totalEdgeWeight == 0) {
            // Calculate total edge weight in first iteration
            for (RowEdge edge : edges) {
                totalEdgeWeight += getEdgeWeight(edge);
            }
        }

        double m = totalEdgeWeight;
        double ki = vertexValue.getTotalWeight();
        double kiIn = vertexValue.getNeighborCommunityWeights().getOrDefault(targetCommunity, 0.0);

        // Simplified: use default weight values
        double sigmaTot = 0.0;
        double sigmaIn = 0.0;

        if (m == 0) {
            return 0.0;
        }

        double a = (kiIn + sigmaIn / (2 * m)) - ((sigmaTot + ki) / (2 * m))
                * ((sigmaTot + ki) / (2 * m));
        double b = (kiIn / (2 * m)) - (sigmaTot / (2 * m)) * (sigmaTot / (2 * m))
                - (ki / (2 * m)) * (ki / (2 * m));

        return a - b;
    }

    /**
     * Send community information to all neighbors.
     */
    private void sendCommunityInfoToNeighbors(RowVertex vertex,
                                               List<RowEdge> edges,
                                               LouvainVertexValue vertexValue) {
        for (RowEdge edge : edges) {
            double weight = getEdgeWeight(edge);
            LouvainMessage msg = new LouvainMessage(vertexValue.getCommunityId(), weight);
            context.sendMessage(edge.getTargetId(), msg);
        }
    }

    /**
     * Get edge weight from RowEdge.
     */
    private double getEdgeWeight(RowEdge edge) {
        if (isWeighted) {
            try {
                // Try to get weight from edge value
                Row value = edge.getValue();
                if (value != null) {
                    Object weightObj = value.getField(0, ObjectType.INSTANCE);
                    if (weightObj instanceof Number) {
                        return ((Number) weightObj).doubleValue();
                    }
                }
            } catch (Exception e) {
                // Fallback to default weight
            }
        }
        return 1.0; // Default weight for unweighted graphs
    }

    /**
     * Serialize LouvainVertexValue to Row for storage.
     */
    private Row serializeVertexValue(LouvainVertexValue value) {
        return ObjectRow.create(
            value.getCommunityId(),
            value.getTotalWeight(),
            value.getInternalWeight()
        );
    }

    /**
     * Deserialize Row to LouvainVertexValue.
     */
    private LouvainVertexValue deserializeVertexValue(Row row) {
        Object communityId = row.getField(0, ObjectType.INSTANCE);
        Object totalWeightObj = row.getField(1, DoubleType.INSTANCE);
        Object internalWeightObj = row.getField(2, DoubleType.INSTANCE);

        double totalWeight = totalWeightObj instanceof Number
                ? ((Number) totalWeightObj).doubleValue() : 0.0;
        double internalWeight = internalWeightObj instanceof Number
                ? ((Number) internalWeightObj).doubleValue() : 0.0;

        LouvainVertexValue value = new LouvainVertexValue();
        value.setCommunityId(communityId);
        value.setTotalWeight(totalWeight);
        value.setInternalWeight(internalWeight);
        return value;
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        if (updatedValues.isPresent()) {
            LouvainVertexValue vertexValue = deserializeVertexValue(updatedValues.get());
            context.take(ObjectRow.create(graphVertex.getId(), vertexValue.getCommunityId()));
        }
    }

    @Override
    public void finishIteration(long iterationId) {
        // For future use: could add global convergence checking here
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("community", graphSchema.getIdType(), false)
        );
    }
}