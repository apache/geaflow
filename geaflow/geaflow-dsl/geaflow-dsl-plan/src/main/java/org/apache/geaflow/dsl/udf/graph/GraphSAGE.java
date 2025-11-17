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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.geaflow.common.config.ConfigHelper;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
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
import org.apache.geaflow.dsl.udf.graph.FeatureReducer;
import org.apache.geaflow.infer.InferContext;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphSAGE algorithm implementation for GQL CALL syntax.
 *
 * <p>This class implements AlgorithmUserFunction to enable GraphSAGE to be called
 * via GQL CALL syntax: CALL GRAPHSAGE([numSamples, [numLayers]]) YIELD (vid, embedding)
 *
 * <p>This implementation:
 * - Uses AlgorithmRuntimeContext for graph access
 * - Creates InferContext for Python model inference
 * - Implements neighbor sampling and feature collection
 * - Calls Python model for embedding generation
 * - Returns vertex ID and embedding vector
 *
 * <p>Note: This requires Python inference environment to be enabled:
 * - geaflow.infer.env.enable=true
 * - geaflow.infer.env.user.transform.classname=GraphSAGETransFormFunction
 */
@Description(name = "graphsage", description = "built-in udga for GraphSAGE node embedding")
public class GraphSAGE implements AlgorithmUserFunction<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphSAGE.class);

    private AlgorithmRuntimeContext<Object, Object> context;
    private InferContext<List<Double>> inferContext;
    private FeatureReducer featureReducer;
    
    // Algorithm parameters
    private int numSamples = 10;  // Number of neighbors to sample per layer
    private int numLayers = 2;    // Number of GraphSAGE layers
    private static final int DEFAULT_REDUCED_DIMENSION = 64;
    
    // Random number generator for neighbor sampling
    private static final Random RANDOM = new Random(42L);
    
    // Cache for neighbor features: neighborId -> features
    // This cache is populated in the first iteration when we sample neighbors
    private final Map<Object, List<Double>> neighborFeaturesCache = new HashMap<>();

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;
        
        // Parse parameters
        if (parameters.length > 0) {
            this.numSamples = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            this.numLayers = Integer.parseInt(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support up to 2 arguments: numSamples, numLayers. "
                    + "Usage: CALL GRAPHSAGE([numSamples, [numLayers]])");
        }
        
        // Initialize feature reducer
        int[] importantDims = new int[DEFAULT_REDUCED_DIMENSION];
        for (int i = 0; i < DEFAULT_REDUCED_DIMENSION; i++) {
            importantDims[i] = i;
        }
        this.featureReducer = new FeatureReducer(importantDims);
        
        // Initialize Python inference context if enabled
        try {
            boolean inferEnabled = ConfigHelper.getBooleanOrDefault(
                context.getConfig().getConfigMap(),
                FrameworkConfigKeys.INFER_ENV_ENABLE.getKey(),
                false);
            
            if (inferEnabled) {
                this.inferContext = new InferContext<>(context.getConfig());
                LOGGER.info("GraphSAGE initialized with numSamples={}, numLayers={}, Python inference enabled",
                    numSamples, numLayers);
            } else {
                LOGGER.warn("GraphSAGE requires Python inference environment. "
                    + "Please set geaflow.infer.env.enable=true");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Python inference context", e);
            throw new RuntimeException("GraphSAGE requires Python inference environment", e);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        updatedValues.ifPresent(vertex::setValue);
        
        long iterationId = context.getCurrentIterationId();
        Object vertexId = vertex.getId();
        
        if (iterationId == 1L) {
            // First iteration: sample neighbors and collect features
            List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
            List<RowEdge> inEdges = context.loadEdges(EdgeDirection.IN);
            
            // Combine all edges (undirected graph)
            List<RowEdge> allEdges = new ArrayList<>();
            allEdges.addAll(outEdges);
            allEdges.addAll(inEdges);
            
            // Sample neighbors for each layer
            Map<Integer, List<Object>> sampledNeighbors = sampleNeighbors(vertexId, allEdges);
            
            // Collect and cache neighbor features from edges
            // In GraphSAGE, neighbor features are typically stored in the graph
            // We'll try to extract them from edges or use the current vertex's approach
            cacheNeighborFeatures(sampledNeighbors, allEdges);
            
            // Store sampled neighbors in vertex value for next iteration
            Map<String, Object> vertexData = new HashMap<>();
            vertexData.put("sampledNeighbors", sampledNeighbors);
            context.updateVertexValue(ObjectRow.create(vertexData));
            
            // Send message to sampled neighbors to activate them
            // The message contains the current vertex's features so neighbors can use them
            List<Double> currentFeatures = getVertexFeatures(vertex);
            for (int layer = 1; layer <= numLayers; layer++) {
                List<Object> layerNeighbors = sampledNeighbors.get(layer);
                if (layerNeighbors != null) {
                    for (Object neighborId : layerNeighbors) {
                        // Send vertex ID and features as message
                        Map<String, Object> messageData = new HashMap<>();
                        messageData.put("senderId", vertexId);
                        messageData.put("features", currentFeatures);
                        context.sendMessage(neighborId, messageData);
                    }
                }
            }
            
        } else if (iterationId == 2L) {
            // Second iteration: neighbors receive messages and can update cache
            // Process messages to extract neighbor features and update cache
            while (messages.hasNext()) {
                Object message = messages.next();
                if (message instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> messageData = (Map<String, Object>) message;
                    Object senderId = messageData.get("senderId");
                    Object features = messageData.get("features");
                    if (senderId != null && features instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Double> senderFeatures = (List<Double>) features;
                        // Cache the sender's features for later use
                        neighborFeaturesCache.put(senderId, senderFeatures);
                    }
                }
            }
            
            // Get current vertex features and send to neighbors
            List<Double> currentFeatures = getVertexFeatures(vertex);
            
            // Send current vertex features to neighbors who need them
            // This helps populate the cache for other vertices
            Map<String, Object> vertexData = extractVertexData(vertex);
            @SuppressWarnings("unchecked")
            Map<Integer, List<Object>> sampledNeighbors = 
                (Map<Integer, List<Object>>) vertexData.get("sampledNeighbors");
            
            if (sampledNeighbors != null) {
                for (List<Object> layerNeighbors : sampledNeighbors.values()) {
                    for (Object neighborId : layerNeighbors) {
                        Map<String, Object> messageData = new HashMap<>();
                        messageData.put("senderId", vertexId);
                        messageData.put("features", currentFeatures);
                        context.sendMessage(neighborId, messageData);
                    }
                }
            }
            
        } else if (iterationId <= numLayers + 1) {
            // Subsequent iterations: collect neighbor features and compute embedding
            if (inferContext == null) {
                LOGGER.error("Python inference context not available");
                return;
            }
            
            // Process any incoming messages to update cache
            while (messages.hasNext()) {
                Object message = messages.next();
                if (message instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> messageData = (Map<String, Object>) message;
                    Object senderId = messageData.get("senderId");
                    Object features = messageData.get("features");
                    if (senderId != null && features instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Double> senderFeatures = (List<Double>) features;
                        neighborFeaturesCache.put(senderId, senderFeatures);
                    }
                }
            }
            
            // Get vertex features
            List<Double> vertexFeatures = getVertexFeatures(vertex);
            
            // Reduce vertex features
            double[] reducedVertexFeatures;
            try {
                reducedVertexFeatures = featureReducer.reduceFeatures(vertexFeatures);
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Vertex {} features too short, padding with zeros", vertexId);
                int requiredSize = featureReducer.getReducedDimension();
                double[] paddedFeatures = new double[requiredSize];
                for (int i = 0; i < vertexFeatures.size() && i < requiredSize; i++) {
                    paddedFeatures[i] = vertexFeatures.get(i);
                }
                reducedVertexFeatures = paddedFeatures;
            }
            
            // Get sampled neighbors from previous iteration
            Map<String, Object> vertexData = extractVertexData(vertex);
            @SuppressWarnings("unchecked")
            Map<Integer, List<Object>> sampledNeighbors = 
                (Map<Integer, List<Object>>) vertexData.get("sampledNeighbors");
            
            if (sampledNeighbors == null) {
                sampledNeighbors = new HashMap<>();
            }
            
            // Collect neighbor features for each layer
            Map<Integer, List<List<Double>>> neighborFeaturesMap = 
                collectNeighborFeatures(sampledNeighbors);
            
            // Convert reduced vertex features to List<Double>
            List<Double> reducedVertexFeatureList = new ArrayList<>();
            for (double value : reducedVertexFeatures) {
                reducedVertexFeatureList.add(value);
            }
            
            // Call Python model for inference
            try {
                Object[] modelInputs = new Object[]{
                    vertexId,
                    reducedVertexFeatureList,
                    neighborFeaturesMap
                };
                
                List<Double> embedding = inferContext.infer(modelInputs);
                
                // Store embedding in vertex value
                Map<String, Object> resultData = new HashMap<>();
                resultData.put("embedding", embedding);
                context.updateVertexValue(ObjectRow.create(resultData));
                
            } catch (Exception e) {
                LOGGER.error("Failed to compute embedding for vertex {}", vertexId, e);
                // Store empty embedding on error
                Map<String, Object> resultData = new HashMap<>();
                resultData.put("embedding", new ArrayList<Double>());
                context.updateVertexValue(ObjectRow.create(resultData));
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (newValue.isPresent()) {
            try {
                Row valueRow = newValue.get();
                @SuppressWarnings("unchecked")
                Map<String, Object> vertexData;
                
                // Try to extract Map from Row
                try {
                    vertexData = (Map<String, Object>) valueRow.getField(0,
                        ObjectType.INSTANCE);
                } catch (Exception e) {
                    // If that fails, try to get from vertex value directly
                    Object vertexValue = vertex.getValue();
                    if (vertexValue instanceof Map) {
                        vertexData = (Map<String, Object>) vertexValue;
                    } else {
                        LOGGER.warn("Cannot extract vertex data for vertex {}", vertex.getId());
                        return;
                    }
                }
                
                if (vertexData != null) {
                    @SuppressWarnings("unchecked")
                    List<Double> embedding = (List<Double>) vertexData.get("embedding");
                    
                    if (embedding != null && !embedding.isEmpty()) {
                        // Output: (vid, embedding)
                        // Embedding is converted to a string representation for output
                        String embeddingStr = embedding.toString();
                        context.take(ObjectRow.create(vertex.getId(), embeddingStr));
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Failed to output result for vertex {}", vertex.getId(), e);
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vid", graphSchema.getIdType(), false),
            new TableField("embedding", org.apache.geaflow.common.type.primitive.StringType.INSTANCE, false)
        );
    }

    @Override
    public void finish() {
        // Clean up Python inference context
        if (inferContext != null) {
            try {
                inferContext.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close inference context", e);
            }
        }
        
        // Clear cache to free memory
        neighborFeaturesCache.clear();
    }

    /**
     * Sample neighbors for each layer.
     */
    private Map<Integer, List<Object>> sampleNeighbors(Object vertexId, List<RowEdge> edges) {
        Map<Integer, List<Object>> sampledNeighbors = new HashMap<>();
        
        // Extract unique neighbor IDs
        List<Object> allNeighbors = new ArrayList<>();
        for (RowEdge edge : edges) {
            Object neighborId = edge.getTargetId();
            if (!neighborId.equals(vertexId) && !allNeighbors.contains(neighborId)) {
                allNeighbors.add(neighborId);
            }
        }
        
        // Sample neighbors for each layer
        for (int layer = 1; layer <= numLayers; layer++) {
            List<Object> layerNeighbors = sampleFixedSize(allNeighbors, numSamples);
            sampledNeighbors.put(layer, layerNeighbors);
        }
        
        return sampledNeighbors;
    }

    /**
     * Sample a fixed number of elements from a list.
     */
    private List<Object> sampleFixedSize(List<Object> list, int size) {
        if (list.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Object> sampled = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int index = RANDOM.nextInt(list.size());
            sampled.add(list.get(index));
        }
        return sampled;
    }

    /**
     * Extract vertex data from vertex value.
     *
     * <p>Helper method to safely extract Map<String, Object> from vertex value,
     * handling both Row and Map types.
     *
     * @param vertex The vertex to extract data from
     * @return Map containing vertex data, or empty map if extraction fails
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractVertexData(RowVertex vertex) {
        Object vertexValue = vertex.getValue();
        if (vertexValue instanceof Row) {
            try {
                return (Map<String, Object>) ((Row) vertexValue).getField(0,
                    ObjectType.INSTANCE);
            } catch (Exception e) {
                LOGGER.warn("Failed to extract vertex data from Row, using empty map", e);
                return new HashMap<>();
            }
        } else if (vertexValue instanceof Map) {
            return (Map<String, Object>) vertexValue;
        } else {
            return new HashMap<>();
        }
    }
    
    /**
     * Get vertex features from vertex value.
     *
     * <p>This method extracts features from the vertex value, handling multiple formats:
     * - Direct List<Double> value
     * - Map with "features" key containing List<Double>
     * - Row with features in first field
     *
     * @param vertex The vertex to extract features from
     * @return List of features, or empty list if not found
     */
    @SuppressWarnings("unchecked")
    private List<Double> getVertexFeatures(RowVertex vertex) {
        Object value = vertex.getValue();
        if (value == null) {
            return new ArrayList<>();
        }
        
        // Try to extract features from vertex value
        // Vertex value might be a List<Double> directly, or wrapped in a Map
        if (value instanceof List) {
            return (List<Double>) value;
        } else if (value instanceof Map) {
            Map<String, Object> vertexData = (Map<String, Object>) value;
            Object features = vertexData.get("features");
            if (features instanceof List) {
                return (List<Double>) features;
            }
        }
        
        // Default: return empty list (will be padded with zeros)
        return new ArrayList<>();
    }

    /**
     * Collect neighbor features for each layer.
     */
    private Map<Integer, List<List<Double>>> collectNeighborFeatures(
        Map<Integer, List<Object>> sampledNeighbors) {
        
        Map<Integer, List<List<Double>>> neighborFeaturesMap = new HashMap<>();
        
        for (Map.Entry<Integer, List<Object>> entry : sampledNeighbors.entrySet()) {
            int layer = entry.getKey();
            List<Object> neighborIds = entry.getValue();
            
            List<List<Double>> layerNeighborFeatures = new ArrayList<>();
            
            for (Object neighborId : neighborIds) {
                // Get neighbor vertex (simplified - in real scenario would query graph)
                // For now, we'll create placeholder features
                List<Double> neighborFeatures = getNeighborFeatures(neighborId);
                
                // Reduce neighbor features
                double[] reducedFeatures;
                try {
                    reducedFeatures = featureReducer.reduceFeatures(neighborFeatures);
                } catch (IllegalArgumentException e) {
                    int requiredSize = featureReducer.getReducedDimension();
                    reducedFeatures = new double[requiredSize];
                    for (int i = 0; i < neighborFeatures.size() && i < requiredSize; i++) {
                        reducedFeatures[i] = neighborFeatures.get(i);
                    }
                }
                
                // Convert to List<Double>
                List<Double> reducedFeatureList = new ArrayList<>();
                for (double value : reducedFeatures) {
                    reducedFeatureList.add(value);
                }
                
                layerNeighborFeatures.add(reducedFeatureList);
            }
            
            neighborFeaturesMap.put(layer, layerNeighborFeatures);
        }
        
        return neighborFeaturesMap;
    }

    /**
     * Cache neighbor features from edges in the first iteration.
     *
     * <p>This method extracts neighbor features from edges or uses a default strategy.
     * In production, neighbor features should be retrieved from the graph state.
     *
     * @param sampledNeighbors Map of layer to sampled neighbor IDs
     * @param edges All edges connected to the current vertex
     */
    private void cacheNeighborFeatures(Map<Integer, List<Object>> sampledNeighbors,
                                      List<RowEdge> edges) {
        // Build a map of neighbor ID to edges for quick lookup
        Map<Object, RowEdge> neighborEdgeMap = new HashMap<>();
        for (RowEdge edge : edges) {
            Object neighborId = edge.getTargetId();
            if (!neighborEdgeMap.containsKey(neighborId)) {
                neighborEdgeMap.put(neighborId, edge);
            }
        }
        
        // For each sampled neighbor, try to extract features
        for (Map.Entry<Integer, List<Object>> entry : sampledNeighbors.entrySet()) {
            for (Object neighborId : entry.getValue()) {
                if (!neighborFeaturesCache.containsKey(neighborId)) {
                    // Try to get features from edge value
                    RowEdge edge = neighborEdgeMap.get(neighborId);
                    List<Double> features = extractFeaturesFromEdge(neighborId, edge);
                    neighborFeaturesCache.put(neighborId, features);
                }
            }
        }
    }
    
    /**
     * Extract features from edge or use default strategy.
     *
     * <p>In a production implementation, this would:
     * 1. Query the graph state for the neighbor vertex
     * 2. Extract features from the vertex value
     * 3. Handle cases where vertex is not found or has no features
     *
     * <p>For now, we use a placeholder that returns empty features.
     * The actual features should be retrieved when the neighbor vertex is processed.
     *
     * @param neighborId The neighbor vertex ID
     * @param edge The edge connecting to the neighbor (may be null)
     * @return List of features for the neighbor
     */
    private List<Double> extractFeaturesFromEdge(Object neighborId, RowEdge edge) {
        // In production, we would:
        // 1. Query the graph state for vertex with neighborId
        // 2. Extract features from vertex value
        // 3. Handle missing vertices gracefully
        
        // For now, return empty list (will be padded with zeros)
        // The actual features will be populated when the neighbor vertex is processed
        // in a subsequent iteration
        return new ArrayList<>();
    }
    
    /**
     * Get neighbor features from cache or extract from messages.
     *
     * <p>This method implements a production-ready strategy for getting neighbor features:
     * 1. First, check the cache populated in iteration 1
     * 2. If not in cache, try to extract from messages (neighbors may have sent their features)
     * 3. If still not found, return empty list (will be padded with zeros)
     *
     * <p>In a full production implementation, this would also:
     * - Query the graph state directly for the neighbor vertex
     * - Handle vertex schema variations
     * - Support different feature storage formats
     *
     * @param neighborId The neighbor vertex ID
     * @param messages Iterator of messages received (may contain neighbor features)
     * @return List of features for the neighbor
     */
    private List<Double> getNeighborFeatures(Object neighborId, Iterator<Object> messages) {
        // Strategy 1: Check cache first (populated in iteration 1)
        if (neighborFeaturesCache.containsKey(neighborId)) {
            List<Double> cachedFeatures = neighborFeaturesCache.get(neighborId);
            if (cachedFeatures != null && !cachedFeatures.isEmpty()) {
                return cachedFeatures;
            }
        }
        
        // Strategy 2: Try to extract from messages
        // In iteration 2+, neighbors may have sent their features as messages
        if (messages != null) {
            while (messages.hasNext()) {
                Object message = messages.next();
                if (message instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> messageData = (Map<String, Object>) message;
                    Object senderId = messageData.get("senderId");
                    if (neighborId.equals(senderId)) {
                        Object features = messageData.get("features");
                        if (features instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Double> neighborFeatures = (List<Double>) features;
                            // Cache for future use
                            neighborFeaturesCache.put(neighborId, neighborFeatures);
                            return neighborFeatures;
                        }
                    }
                }
            }
        }
        
        // Strategy 3: Return empty list (will be padded with zeros in feature reduction)
        // In production, this would trigger a graph state query as a fallback
        LOGGER.debug("No features found for neighbor {}, using empty features", neighborId);
        return new ArrayList<>();
    }
    
    /**
     * Get neighbor features (overloaded method for backward compatibility).
     *
     * <p>This method is called from collectNeighborFeatures where we don't have
     * direct access to messages. It uses the cache populated in iteration 1.
     *
     * @param neighborId The neighbor vertex ID
     * @return List of features for the neighbor
     */
    private List<Double> getNeighborFeatures(Object neighborId) {
        // Use cache populated in iteration 1
        if (neighborFeaturesCache.containsKey(neighborId)) {
            return neighborFeaturesCache.get(neighborId);
        }
        
        // Return empty list (will be padded with zeros)
        LOGGER.debug("Neighbor {} not in cache, using empty features", neighborId);
        return new ArrayList<>();
    }
}

