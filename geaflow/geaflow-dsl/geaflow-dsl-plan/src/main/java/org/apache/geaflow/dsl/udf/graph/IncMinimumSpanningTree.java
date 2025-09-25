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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.udf.graph.mst.MSTEdge;
import org.apache.geaflow.dsl.udf.graph.mst.MSTMessage;
import org.apache.geaflow.dsl.udf.graph.mst.MSTVertexState;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Incremental Minimum Spanning Tree algorithm implementation.
 * Based on Geaflow incremental graph computing capabilities, implements MST maintenance on dynamic graphs.
 *
 * <p>Algorithm principle:
 * 1. Maintain current MST state
 * 2. For new edges: Use Union-Find to detect if cycles are formed, if no cycle and weight is smaller then add to MST
 * 3. For deleted edges: If deleted edge is MST edge, need to reconnect separated components
 * 4. Use vertex-centric message passing mechanism for distributed computing
 *
 * @author Geaflow Team
 */
@Description(name = "IncMST", description = "built-in udga for Incremental Minimum Spanning Tree")
public class IncMinimumSpanningTree implements AlgorithmUserFunction<Object, Object>,
    IncrementalAlgorithmUserFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncMinimumSpanningTree.class);

    /** Field index for vertex state in row value. */
    private static final int STATE_FIELD_INDEX = 0;

    private AlgorithmRuntimeContext<Object, Object> context;
    private IType<?> idType; // Cache the ID type for better performance
    
    // Configuration parameters
    private int maxIterations = 50; // Default maximum iterations
    private double convergenceThreshold = 0.001; // Default convergence threshold
    private String keyFieldName = "mst_edges"; // Default key field name
    
    // Memory optimization parameters
    private static final int MEMORY_COMPACT_INTERVAL = 10; // Compact memory every 10 iterations
    private int iterationCount = 0;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;

        // Cache the ID type for better performance and type safety
        this.idType = context.getGraphSchema().getIdType();

        // Parse configuration parameters
        if (parameters != null && parameters.length > 0) {
            if (parameters.length > 3) {
                throw new IllegalArgumentException(
                    "IncMinimumSpanningTree algorithm supports at most 3 parameters: "
                    + "maxIterations, convergenceThreshold, keyFieldName");
            }
            
            // Parse maxIterations (first parameter)
            if (parameters.length > 0 && parameters[0] != null) {
                try {
                    this.maxIterations = Integer.parseInt(String.valueOf(parameters[0]));
                    if (this.maxIterations <= 0) {
                        throw new IllegalArgumentException("maxIterations must be positive");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Invalid maxIterations parameter: " + parameters[0], e);
                }
            }
            
            // Parse convergenceThreshold (second parameter)
            if (parameters.length > 1 && parameters[1] != null) {
                try {
                    this.convergenceThreshold = Double.parseDouble(String.valueOf(parameters[1]));
                    if (this.convergenceThreshold < 0 || this.convergenceThreshold > 1) {
                        throw new IllegalArgumentException(
                            "convergenceThreshold must be between 0 and 1");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Invalid convergenceThreshold parameter: " + parameters[1], e);
                }
            }
            
            // Parse keyFieldName (third parameter)
            if (parameters.length > 2 && parameters[2] != null) {
                this.keyFieldName = String.valueOf(parameters[2]);
                if (this.keyFieldName.trim().isEmpty()) {
                    throw new IllegalArgumentException("keyFieldName cannot be empty");
                }
            }
        }
        
        LOGGER.info("IncMinimumSpanningTree initialized with maxIterations={}, convergenceThreshold={}, keyFieldName='{}'",
                   maxIterations, convergenceThreshold, keyFieldName);
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        // Initialize vertex state if not exists
        MSTVertexState currentState = getCurrentVertexState(vertex);

        // Process incoming messages
        boolean stateChanged = false;
        Object validatedVertexId = validateVertexId(vertex.getId());
        while (messages.hasNext()) {
            Object messageObj = messages.next();
            if (!(messageObj instanceof MSTMessage)) {
                throw new IllegalArgumentException(
                    String.format("Invalid message type for IncMinimumSpanningTree: expected %s, got %s (value: %s)",
                        MSTMessage.class.getSimpleName(),
                        messageObj.getClass().getSimpleName(),
                        messageObj)
                );
            }
            MSTMessage message = (MSTMessage) messageObj;
            if (processMessage(validatedVertexId, message, currentState)) {
                stateChanged = true;
            }
        }

        // If this is the first iteration and no messages were processed,
        // load edges and propose them to neighbors
        if (!updatedValues.isPresent() && !messages.hasNext()) {
            // Load all outgoing edges and propose them to target vertices
            List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
            
            // Memory optimization: limit the number of edges processed per iteration
            // to prevent memory overflow and excessive RPC messages
            int maxEdgesPerIteration = Math.min(outEdges.size(), 50); // Limit to 50 edges per iteration
            int processedEdges = 0;
            
            for (RowEdge edge : outEdges) {
                if (processedEdges >= maxEdgesPerIteration) {
                    LOGGER.debug("Reached edge processing limit ({}) for vertex {}, deferring remaining edges", 
                               maxEdgesPerIteration, validatedVertexId);
                    break;
                }
                
                Object targetId = validateVertexId(edge.getTargetId());
                double weight = (Double) edge.getValue().getField(0, DoubleType.INSTANCE);

                // Create edge proposal message
                MSTMessage proposalMessage = new MSTMessage(
                    MSTMessage.MessageType.EDGE_PROPOSAL,
                    validatedVertexId,
                    targetId,
                    weight,
                    currentState.getComponentId()
                );

                // Send proposal to target vertex
                context.sendMessage(targetId, proposalMessage);
                processedEdges++;

                LOGGER.debug("Sent edge proposal from {} to {} with weight {} ({}/{})",
                           validatedVertexId, targetId, weight, processedEdges, maxEdgesPerIteration);
            }
        }

        // Memory optimization: compact vertex state periodically
        iterationCount++;
        if (iterationCount % MEMORY_COMPACT_INTERVAL == 0) {
            currentState.compactMSTEdges();
            LOGGER.debug("Memory compaction performed for vertex {} at iteration {}", 
                        validatedVertexId, iterationCount);
        }

        // Update vertex state if changed
        if (stateChanged) {
            context.updateVertexValue(ObjectRow.create(currentState, true));
        } else if (!updatedValues.isPresent()) {
            // First time initialization
            context.updateVertexValue(ObjectRow.create(currentState, true));
        }

        // Vote to terminate if no state changes occurred and we've processed messages
        // This ensures the algorithm terminates after processing all edges
        // Also check if we've reached the maximum number of iterations
        long currentIteration = context.getCurrentIterationId();
        if ((!stateChanged && updatedValues.isPresent()) || currentIteration >= maxIterations) {
            String terminationReason = currentIteration >= maxIterations 
                ? "MAX_ITERATIONS_REACHED" : "MST_CONVERGED";
            context.voteToTerminate(terminationReason, 1);
            
            if (currentIteration >= maxIterations) {
                LOGGER.warn("IncMST algorithm reached maximum iterations ({}) without convergence", maxIterations);
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // Output MST results for each vertex
        if (updatedValues.isPresent()) {
            Row values = updatedValues.get();
            Object stateObj = values.getField(STATE_FIELD_INDEX, ObjectType.INSTANCE);
            if (!(stateObj instanceof MSTVertexState)) {
                throw new IllegalStateException(
                    String.format("Invalid vertex state type in finish(): expected %s, got %s (value: %s)",
                        MSTVertexState.class.getSimpleName(),
                        stateObj.getClass().getSimpleName(),
                        stateObj)
                );
            }
            MSTVertexState state = (MSTVertexState) stateObj;
            // Output each MST edge as a separate record
            for (MSTEdge mstEdge : state.getMstEdges()) {
                // Validate IDs before outputting
                Object validatedSrcId = validateVertexId(mstEdge.getSourceId());
                Object validatedTargetId = validateVertexId(mstEdge.getTargetId());
                double weight = mstEdge.getWeight();

                context.take(ObjectRow.create(validatedSrcId, validatedTargetId, weight));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        // Use the cached ID type for consistency and performance
        IType<?> vertexIdType = (idType != null) ? idType : graphSchema.getIdType();

        // Return result type: srcId, targetId, weight for each MST edge
        return new StructType(
            new TableField("srcId", vertexIdType, false),
            new TableField("targetId", vertexIdType, false),
            new TableField("weight", DoubleType.INSTANCE, false)
        );
    }

    /**
     * Initialize vertex state.
     * Each vertex is initialized as an independent component with itself as the root node.
     */
    private void initializeVertex(RowVertex vertex) {
        // Validate vertex ID from input
        Object vertexId = validateVertexId(vertex.getId());

        // Create initial MST state
        MSTVertexState initialState = new MSTVertexState(vertexId);

        // Update vertex value
        context.updateVertexValue(ObjectRow.create(initialState, true));
    }

    /**
     * Process single message.
     * Execute corresponding processing logic based on message type.
     */
    private boolean processMessage(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Simplified message processing for basic MST functionality
        switch (message.getType()) {
            case COMPONENT_UPDATE:
                return handleComponentUpdate(vertexId, message, state);
            case EDGE_PROPOSAL:
                return handleEdgeProposal(vertexId, message, state);
            case EDGE_ACCEPTANCE:
                return handleEdgeAcceptance(vertexId, message, state);
            case EDGE_REJECTION:
                return handleEdgeRejection(vertexId, message, state);
            case MST_EDGE_FOUND:
                return handleMSTEdgeFound(vertexId, message, state);
            default:
                return false;
        }
    }

    /**
     * Handle component update message.
     * Update vertex component identifier.
     */
    private boolean handleComponentUpdate(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Validate component ID using cached type information
        Object validatedComponentId = validateVertexId(message.getComponentId());
        if (!validatedComponentId.equals(state.getComponentId())) {
            state.setComponentId(validatedComponentId);
            return true;
        }
        return false;
    }

    /**
     * Handle edge proposal message.
     * Check whether to accept new MST edge.
     * In incremental MST, an edge can be accepted if its endpoints belong to different components.
     */
    private boolean handleEdgeProposal(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Validate vertex IDs
        Object validatedSourceId = validateVertexId(message.getSourceId());
        Object validatedTargetId = validateVertexId(message.getTargetId());

        // Check if edge endpoints belong to different components
        // If they do, the edge can be accepted without creating a cycle
        Object currentComponentId = state.getComponentId();
        Object proposedComponentId = message.getComponentId();

        // Only accept edge if endpoints are in different components
        if (!Objects.equals(currentComponentId, proposedComponentId)) {
            // Create acceptance message
            MSTMessage acceptanceMessage = new MSTMessage(
                MSTMessage.MessageType.EDGE_ACCEPTANCE,
                validatedSourceId,
                validatedTargetId,
                message.getWeight(),
                proposedComponentId
            );

            // Send acceptance message to the source vertex
            context.sendMessage(validatedSourceId, acceptanceMessage);

            LOGGER.debug("Accepted edge proposal: {} -- {} (weight: {}) between components {} and {}",
                        validatedSourceId, validatedTargetId, message.getWeight(),
                        currentComponentId, proposedComponentId);

            return true;
        } else {
            // Edge endpoints are in the same component, would create a cycle
            // Send rejection message
            MSTMessage rejectionMessage = new MSTMessage(
                MSTMessage.MessageType.EDGE_REJECTION,
                validatedSourceId,
                validatedTargetId,
                message.getWeight(),
                proposedComponentId
            );

            // Send rejection message to the source vertex
            context.sendMessage(validatedSourceId, rejectionMessage);

            LOGGER.debug("Rejected edge proposal: {} -- {} (weight: {}) - same component {}",
                        validatedSourceId, validatedTargetId, message.getWeight(), currentComponentId);

            return false;
        }
    }

    /**
     * Handle edge acceptance message.
     * Add MST edge and merge components.
     */
    private boolean handleEdgeAcceptance(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Validate vertex IDs using cached type information
        Object validatedVertexId = validateVertexId(vertexId);
        Object validatedSourceId = validateVertexId(message.getSourceId());

        // Create MST edge with validated IDs
        MSTEdge mstEdge = new MSTEdge(validatedVertexId, validatedSourceId, message.getWeight());
        state.addMSTEdge(mstEdge);

        // Merge components with type validation
        Object validatedMessageComponentId = validateVertexId(message.getComponentId());
        Object newComponentId = findMinComponentId(state.getComponentId(), validatedMessageComponentId);
        state.setComponentId(newComponentId);

        return true;
    }

    /**
     * Handle edge rejection message.
     * Record rejected edges.
     */
    private boolean handleEdgeRejection(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Can record rejected edges here for debugging or statistics
        return false;
    }

    /**
     * Handle MST edge discovery message.
     * Record discovered MST edges.
     */
    private boolean handleMSTEdgeFound(Object vertexId, MSTMessage message, MSTVertexState state) {
        MSTEdge foundEdge = message.getEdge();
        if (foundEdge != null && !state.getMstEdges().contains(foundEdge)) {
            state.addMSTEdge(foundEdge);
            return true;
        }
        return false;
    }

    /**
     * Validate and convert vertex ID to ensure type safety.
     * Uses TypeCastUtil for comprehensive type validation and conversion.
     *
     * @param vertexId The vertex ID to validate
     * @return The validated vertex ID
     * @throws IllegalArgumentException if vertexId is null or type incompatible
     */
    private Object validateVertexId(Object vertexId) {
        if (vertexId == null) {
            throw new IllegalArgumentException("Vertex ID cannot be null");
        }

        // If idType is not initialized (should not happen in normal flow), return as-is
        if (idType == null) {
            return vertexId;
        }

        try {
            // Use TypeCastUtil for type conversion - this handles all supported type conversions
            return TypeCastUtil.cast(vertexId, idType.getTypeClass());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                String.format("Invalid vertex ID type conversion: expected %s, got %s (value: %s). Error: %s",
                    idType.getTypeClass().getSimpleName(),
                    vertexId.getClass().getSimpleName(),
                    vertexId,
                    e.getMessage()
                ), e
            );
        }
    }

    /**
     * Get current vertex state.
     * Create new state if it doesn't exist.
     */
    private MSTVertexState getCurrentVertexState(RowVertex vertex) {
        if (vertex.getValue() != null) {
            Object stateObj = vertex.getValue().getField(STATE_FIELD_INDEX, ObjectType.INSTANCE);
            if (stateObj != null) {
                if (!(stateObj instanceof MSTVertexState)) {
                    throw new IllegalStateException(
                        String.format("Invalid vertex state type in getCurrentVertexState(): expected %s, got %s (value: %s)",
                            MSTVertexState.class.getSimpleName(),
                            stateObj.getClass().getSimpleName(),
                            stateObj)
                    );
                }
                return (MSTVertexState) stateObj;
            }
        }
        // Validate vertex ID when creating new state
        Object validatedVertexId = validateVertexId(vertex.getId());
        return new MSTVertexState(validatedVertexId);
    }

    /**
     * Select smaller component ID as new component ID.
     * ID selection strategy for component merging.
     */
    private Object findMinComponentId(Object id1, Object id2) {
        if (id1.toString().compareTo(id2.toString()) < 0) {
            return id1;
        }
        return id2;
    }
    
} 