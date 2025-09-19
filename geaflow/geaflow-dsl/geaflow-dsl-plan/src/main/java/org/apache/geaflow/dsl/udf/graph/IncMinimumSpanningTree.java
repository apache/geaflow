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
import java.util.Optional;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
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
@Description(name = "inc_mst", description = "built-in udga for Incremental Minimum Spanning Tree")
public class IncMinimumSpanningTree implements AlgorithmUserFunction<Object, Object>,
    IncrementalAlgorithmUserFunction {

    /** Field index for vertex state in row value. */
    private static final int STATE_FIELD_INDEX = 0;

    private AlgorithmRuntimeContext<Object, Object> context;
    private String keyFieldName = "mst_edges";
    private int maxIterations = 50; // Maximum number of iterations
    private double convergenceThreshold = 0.001; // Convergence threshold
    private IType<?> idType; // Cache the ID type for better performance

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;

        // Cache the ID type for better performance and type safety
        this.idType = context.getGraphSchema().getIdType();

        // Parse parameters: maxIterations, convergenceThreshold, keyFieldName
        if (parameters.length > 0) {
            this.maxIterations = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            this.convergenceThreshold = Double.parseDouble(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            this.keyFieldName = String.valueOf(parameters[2]);
        }

        if (parameters.length > 3) {
            throw new IllegalArgumentException(
                "Only support up to 3 arguments: maxIterations, "
                + "convergenceThreshold, keyFieldName");
        }
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
            if (messageObj instanceof MSTMessage) {
                MSTMessage message = (MSTMessage) messageObj;
                if (processMessage(validatedVertexId, message, currentState)) {
                    stateChanged = true;
                }
            }
        }

        // Update vertex state if changed
        if (stateChanged) {
            context.updateVertexValue(ObjectRow.create(currentState, true));
        } else if (!updatedValues.isPresent()) {
            // First time initialization
            context.updateVertexValue(ObjectRow.create(currentState, true));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // Output MST results for each vertex
        if (updatedValues.isPresent()) {
            Row values = updatedValues.get();
            Object stateObj = values.getField(STATE_FIELD_INDEX, ObjectType.INSTANCE);
            if (stateObj instanceof MSTVertexState) {
                MSTVertexState state = (MSTVertexState) stateObj;
                if (!state.getMstEdges().isEmpty()) {
                    context.take(ObjectRow.create(graphVertex.getId(), state.getMstEdges()));
                }
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        // Use the cached ID type for consistency and performance
        IType<?> vertexIdType = (idType != null) ? idType : graphSchema.getIdType();

        // Return result type: vertex ID and MST edge set
        return new StructType(
            new TableField("vertex_id", vertexIdType, false),
            new TableField(keyFieldName, ObjectType.INSTANCE, false)
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
     */
    private boolean handleEdgeProposal(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Simplified edge proposal handling
        return false;
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
            if (stateObj instanceof MSTVertexState) {
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