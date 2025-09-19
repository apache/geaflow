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

import java.io.Serializable;
import java.util.*;
import org.apache.geaflow.common.type.primitive.IntegerType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Incremental K-Core algorithm implementation.
 * Supports incremental updates for dynamic graphs, handling edge addition and deletion scenarios.
 * 
 * <p>Algorithm principle:
 * 1. Maintain vertex K-Core value cache and degree information
 * 2. When graph changes, only recalculate affected vertices
 * 3. Propagate recalculation requests between neighbors through message passing mechanism
 * 4. Implement intelligent pruning to avoid unnecessary recalculations
 * 
 * @author Geaflow
 */
@Description(name = "incremental_kcore", description = "built-in udga for Incremental K-Core")
public class IncrementalKCore implements AlgorithmUserFunction<Object, IncrementalKCore.KCoreMessage>,
    IncrementalAlgorithmUserFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalKCore.class);
    
    private AlgorithmRuntimeContext<Object, KCoreMessage> context;
    private int k = 3; // Default K value
    private int maxIterations = 100; // Maximum number of iterations
    private double convergenceThreshold = 0.001; // Convergence threshold
    
    // Vertex state cache
    private Map<Object, Integer> vertexCoreValues = new HashMap<>();
    private Map<Object, Integer> vertexDegrees = new HashMap<>();
    private Set<Object> affectedVertices = new HashSet<>();
    private long currentIteration = 0L; // Current iteration number
    
    @Override
    public void init(AlgorithmRuntimeContext<Object, KCoreMessage> context, Object[] parameters) {
        this.context = context;
        
        // Parse parameters: k, maxIterations, convergenceThreshold
        if (parameters.length > 0) {
            this.k = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            this.maxIterations = Integer.parseInt(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            this.convergenceThreshold = Double.parseDouble(String.valueOf(parameters[2]));
        }
        
        if (parameters.length > 3) {
            throw new IllegalArgumentException(
                "Only support up to 3 arguments: k, maxIterations, convergenceThreshold");
        }
        
        LOGGER.info("Incremental K-Core initialized with k={}, maxIterations={}, threshold={}", 
                   k, maxIterations, convergenceThreshold);
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<KCoreMessage> messages) {
        updatedValues.ifPresent(vertex::setValue);

        currentIteration = context.getCurrentIterationId();

        // First iteration: initialization
        if (currentIteration == 1L) {
            initializeVertex(vertex);
            return;
        }

        // Check if reached maximum iterations
        if (currentIteration > maxIterations) {
            return;
        }

        // Process incremental updates and K-Core computation
        processIncrementalUpdate(vertex, messages);
    }
    
    /**
     * Initialize vertex state.
     * Calculate initial degree and K-Core value.
     */
    private void initializeVertex(RowVertex vertex) {
        Object vertexId = vertex.getId();
        
        // Calculate initial degree (including static and dynamic edges)
        List<RowEdge> staticEdges = context.loadStaticEdges(EdgeDirection.BOTH);
        List<RowEdge> dynamicEdges = context.loadDynamicEdges(EdgeDirection.BOTH);
        
        int totalDegree = staticEdges.size() + dynamicEdges.size();
        vertexDegrees.put(vertexId, totalDegree);
        
        // Initialize K-Core value to degree (will be refined during iterations)
        // In the true K-Core algorithm, all vertices have core values equal to their degree in the first round
        int initialCore = totalDegree;
        vertexCoreValues.put(vertexId, initialCore);
        
        // Update vertex value: core_value, degree, change_status
        context.updateVertexValue(ObjectRow.create(initialCore, totalDegree, "INIT"));
        
        // Send initialization messages to neighbors
        List<RowEdge> allEdges = new ArrayList<>();
        allEdges.addAll(staticEdges);
        allEdges.addAll(dynamicEdges);
        
        sendMessageToNeighbors(allEdges, new KCoreMessage(vertexId, initialCore, KCoreMessage.MessageType.INIT));
        
        LOGGER.debug("Initialized vertex {} with degree={}, core={}", vertexId, totalDegree, initialCore);
    }
    
    /**
     * Process incremental update messages and perform K-Core computation.
     * Handle different message types accordingly.
     */
    private void processIncrementalUpdate(RowVertex vertex, Iterator<KCoreMessage> messages) {
        Object vertexId = vertex.getId();
        boolean needsRecomputation = false;
        Set<Object> changedNeighbors = new HashSet<>();
        Map<Object, Integer> neighborCores = new HashMap<>();

        // Process received messages
        while (messages.hasNext()) {
            KCoreMessage message = messages.next();

            switch (message.getType()) {
                case EDGE_ADDED:
                    handleEdgeAdded(vertexId, message);
                    needsRecomputation = true;
                    break;
                case EDGE_REMOVED:
                    handleEdgeRemoved(vertexId, message);
                    needsRecomputation = true;
                    break;
                case CORE_CHANGED:
                    changedNeighbors.add(message.getSourceId());
                    neighborCores.put(message.getSourceId(), message.getCoreValue());
                    needsRecomputation = true;
                    break;
                case INIT:
                    neighborCores.put(message.getSourceId(), message.getCoreValue());
                    break;
                default:
                    LOGGER.warn("Unknown message type: {}", message.getType());
                    break;
            }
        }

        // Always perform K-Core computation if we have neighbor information
        if (!neighborCores.isEmpty() || needsRecomputation) {
            recomputeKCore(vertex, neighborCores);
        }
    }
    
    /**
     * Handle edge addition event.
     * Increase vertex degree and mark as affected vertex.
     */
    private void handleEdgeAdded(Object vertexId, KCoreMessage message) {
        // Increase degree
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);
        vertexDegrees.put(vertexId, currentDegree + 1);
        affectedVertices.add(vertexId);
        
        LOGGER.debug("Edge added to vertex {}, new degree={}", vertexId, currentDegree + 1);
    }
    
    /**
     * Handle edge deletion event.
     * Decrease vertex degree and mark as affected vertex.
     */
    private void handleEdgeRemoved(Object vertexId, KCoreMessage message) {
        // Decrease degree
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);
        vertexDegrees.put(vertexId, Math.max(0, currentDegree - 1));
        affectedVertices.add(vertexId);
        
        LOGGER.debug("Edge removed from vertex {}, new degree={}", vertexId, currentDegree - 1);
    }
    
    /**
     * Recompute K-Core value.
     * Use standard K-Core algorithm logic: calculate valid neighbor count (neighbors with K-Core value >= k)
     */
    private void recomputeKCore(RowVertex vertex, Map<Object, Integer> neighborCores) {
        Object vertexId = vertex.getId();
        int currentCore = vertexCoreValues.getOrDefault(vertexId, 0);
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);

        // Calculate valid neighbor count: number of neighbors with K-Core value >= k
        int validNeighborCount = countValidNeighbors(vertex, neighborCores);

        // Core logic of K-Core algorithm:
        // New K-Core value = min(valid neighbor count, current degree)
        int newCore = Math.min(validNeighborCount, currentDegree);

        // If the core value is less than k, the vertex doesn't belong to k-core
        if (newCore < k) {
            newCore = 0;
        }

        // If core value changes, update state and broadcast changes
        if (newCore != currentCore) {
            vertexCoreValues.put(vertexId, newCore);
            affectedVertices.add(vertexId);

            // Update vertex value with correct change status
            String changeType = (currentIteration == 1L) ? "INIT" :
                              (newCore > currentCore ? "INCREASED" : "DECREASED");
            context.updateVertexValue(ObjectRow.create(newCore, currentDegree, changeType));

            // Broadcast changes to neighbors
            List<RowEdge> allEdges = new ArrayList<>();
            allEdges.addAll(context.loadStaticEdges(EdgeDirection.BOTH));
            allEdges.addAll(context.loadDynamicEdges(EdgeDirection.BOTH));

            sendMessageToNeighbors(allEdges, new KCoreMessage(vertexId, newCore, KCoreMessage.MessageType.CORE_CHANGED));

            LOGGER.debug("Vertex {} core changed from {} to {} (valid neighbors: {}, degree: {}, k: {})",
                        vertexId, currentCore, newCore, validNeighborCount, currentDegree, k);
        } else {
            // Even if core value didn't change, update with UNCHANGED status if it's not the first iteration
            if (currentIteration > 1L) {
                context.updateVertexValue(ObjectRow.create(newCore, currentDegree, "UNCHANGED"));
            }
        }
    }
    
    /**
     * Calculate valid neighbor count.
     * Count neighbors with K-Core value >= k.
     */
    private int countValidNeighbors(RowVertex vertex, Map<Object, Integer> neighborCores) {
        List<RowEdge> allEdges = new ArrayList<>();
        allEdges.addAll(context.loadStaticEdges(EdgeDirection.BOTH));
        allEdges.addAll(context.loadDynamicEdges(EdgeDirection.BOTH));
        
        int validCount = 0;
        for (RowEdge edge : allEdges) {
            Object neighborId = edge.getTargetId().equals(vertex.getId()) 
                ? edge.getSrcId() : edge.getTargetId();
            
            int neighborCore = neighborCores.getOrDefault(neighborId, 
                vertexCoreValues.getOrDefault(neighborId, 0));
            
            if (neighborCore >= k) {
                validCount++;
            }
        }
        
        return validCount;
    }
    
    /**
     * Send message to all neighbors.
     * Traverse all edges and send messages to neighbor vertices.
     *
     * @param edges List of edges to traverse
     * @param message Message to send to neighbors
     */
    private void sendMessageToNeighbors(List<RowEdge> edges, KCoreMessage message) {
        Object sourceId = message.getSourceId();
        Set<Object> sentTo = new HashSet<>(); // Avoid duplicate message sending

        for (RowEdge edge : edges) {
            Object targetId = edge.getTargetId();
            Object srcId = edge.getSrcId();

            // Determine neighbor node
            Object neighborId = targetId.equals(sourceId) ? srcId : targetId;

            // Only send messages to neighbors, avoiding duplicates
            if (!neighborId.equals(sourceId) && !sentTo.contains(neighborId)) {
                context.sendMessage(neighborId, message);
                sentTo.add(neighborId);
                LOGGER.debug("Sent {} message from {} to {}", message.getType(), sourceId, neighborId);
            }
        }
    }


    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(vertex::setValue);

        Object vertexId = vertex.getId();
        int coreValue = vertexCoreValues.getOrDefault(vertexId, 0);
        int degree = vertexDegrees.getOrDefault(vertexId, 0);

        // Determine change status based on whether vertex was affected during computation
        String changeStatus;
        if (currentIteration == 1L) {
            changeStatus = "INIT";
        } else if (affectedVertices.contains(vertexId)) {
            changeStatus = "CHANGED";
        } else {
            changeStatus = "UNCHANGED";
        }

        context.take(ObjectRow.create(vertexId, coreValue, degree, changeStatus));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vid", graphSchema.getIdType(), false),
            new TableField("core_value", IntegerType.INSTANCE, false),
            new TableField("degree", IntegerType.INSTANCE, false),
            new TableField("change_status", StringType.INSTANCE, false)
        );
    }
    
    /**
     * K-Core message class.
     * Used for inter-vertex communication.
     */
    public static class KCoreMessage implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        /** Message type enumeration. */
        public enum MessageType {
            INIT,           // Initialization message
            EDGE_ADDED,     // Edge addition message
            EDGE_REMOVED,   // Edge removal message
            CORE_CHANGED    // Core value change message
        }
        
        private Object sourceId;
        private int coreValue;
        private MessageType type;
        private Object edgeInfo; // Optional edge information
        private long timestamp;
        
        public KCoreMessage(Object sourceId, int coreValue, MessageType type) {
            this.sourceId = sourceId;
            this.coreValue = coreValue;
            this.type = type;
            this.timestamp = System.currentTimeMillis();
        }
        
        public KCoreMessage(Object sourceId, int coreValue, MessageType type, Object edgeInfo) {
            this(sourceId, coreValue, type);
            this.edgeInfo = edgeInfo;
        }
        
        // Getters and Setters
        public Object getSourceId() {
            return sourceId;
        }
        
        public void setSourceId(Object sourceId) {
            this.sourceId = sourceId;
        }
        
        public int getCoreValue() {
            return coreValue;
        }
        
        public void setCoreValue(int coreValue) {
            this.coreValue = coreValue;
        }
        
        public MessageType getType() {
            return type;
        }
        
        public void setType(MessageType type) {
            this.type = type;
        }
        
        public Object getEdgeInfo() {
            return edgeInfo;
        }
        
        public void setEdgeInfo(Object edgeInfo) {
            this.edgeInfo = edgeInfo;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return "KCoreMessage{"
                + "sourceId=" + sourceId
                + ", coreValue=" + coreValue
                + ", type=" + type
                + ", edgeInfo=" + edgeInfo
                + ", timestamp=" + timestamp
                + '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            KCoreMessage that = (KCoreMessage) obj;
            return coreValue == that.coreValue
                   && timestamp == that.timestamp
                   && Objects.equals(sourceId, that.sourceId)
                   && type == that.type
                   && Objects.equals(edgeInfo, that.edgeInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceId, coreValue, type, edgeInfo, timestamp);
        }
    }
}
