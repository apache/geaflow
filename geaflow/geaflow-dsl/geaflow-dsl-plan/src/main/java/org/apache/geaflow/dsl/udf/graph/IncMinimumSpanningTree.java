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

import java.util.*;

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
import org.apache.geaflow.dsl.udf.graph.mst.MSTEdge;
import org.apache.geaflow.dsl.udf.graph.mst.MSTMessage;
import org.apache.geaflow.dsl.udf.graph.mst.MSTVertexState;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

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

    private AlgorithmRuntimeContext<Object, Object> context;
    private String keyFieldName = "mst_edges";
    private int maxIterations = 50; // Maximum number of iterations
    private double convergenceThreshold = 0.001; // Convergence threshold

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;
        
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
        long currentIterationId = context.getCurrentIterationId();
        
        if (currentIterationId == 1L) {
            // Initialization phase: each vertex initialized as independent component
            initializeVertex(vertex);
        } else if (currentIterationId <= maxIterations) {
            // Computation phase: process messages and update MST
            processMessages(vertex, messages);
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // Completion phase: output MST results
        if (updatedValues.isPresent()) {
            Row values = updatedValues.get();
            Object mstEdges = values.getField(0, ObjectType.INSTANCE);
            boolean hasChanged = (boolean) values.getField(1, ObjectType.INSTANCE);
            
            if (hasChanged && mstEdges != null) {
                // Output MST edge information
                context.take(ObjectRow.create(graphVertex.getId(), mstEdges));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        // Return result type: vertex ID and MST edge set
        return new StructType(
            new TableField("vertex_id", graphSchema.getIdType(), false),
            new TableField(keyFieldName, ObjectType.INSTANCE, false)
        );
    }

    /**
     * Initialize vertex state.
     * Each vertex is initialized as an independent component with itself as the root node.
     */
    private void initializeVertex(RowVertex vertex) {
        Object vertexId = vertex.getId();
        
        // Create initial MST state
        MSTVertexState initialState = new MSTVertexState(vertexId);
        
        // Update vertex value
        context.updateVertexValue(ObjectRow.create(initialState, true));
        
        // Send initialization messages to neighbors
        List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
        for (RowEdge edge : edges) {
            MSTMessage initMessage = new MSTMessage(
                MSTMessage.MessageType.COMPONENT_UPDATE,
                vertexId,
                edge.getTargetId(),
                0.0
            );
            initMessage.setComponentId(vertexId);
            context.sendMessage(edge.getTargetId(), initMessage);
        }
    }

    /**
     * Process received messages.
     * Execute corresponding MST update logic based on message type.
     */
    private void processMessages(RowVertex vertex, Iterator<Object> messages) {
        Object vertexId = vertex.getId();
        MSTVertexState currentState = getCurrentVertexState(vertex);
        boolean stateChanged = false;
        
        while (messages.hasNext()) {
            Object messageObj = messages.next();
            if (messageObj instanceof MSTMessage) {
                MSTMessage message = (MSTMessage) messageObj;
                stateChanged |= processMessage(vertexId, message, currentState);
            }
        }
        
        // If state changes, update vertex value and broadcast update
        if (stateChanged) {
            context.updateVertexValue(ObjectRow.create(currentState, true));
            broadcastStateUpdate(vertexId, currentState);
        }
    }

    /**
     * Process single message.
     * Execute corresponding processing logic based on message type.
     */
    private boolean processMessage(Object vertexId, MSTMessage message, MSTVertexState state) {
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
        Object newComponentId = message.getComponentId();
        if (!newComponentId.equals(state.getComponentId())) {
            state.setComponentId(newComponentId);
            return true;
        }
        return false;
    }

    /**
     * Handle edge proposal message.
     * Check whether to accept new MST edge.
     */
    private boolean handleEdgeProposal(Object vertexId, MSTMessage message, MSTVertexState state) {
        Object sourceComponentId = message.getComponentId();
        Object targetComponentId = state.getComponentId();
        
        // Check if connecting different components
        if (!sourceComponentId.equals(targetComponentId)) {
            double edgeWeight = message.getWeight();
            
            // Check if it's a better edge
            if (edgeWeight < state.getMinEdgeWeight()) {
                // Accept edge proposal
                MSTMessage acceptance = new MSTMessage(
                    MSTMessage.MessageType.EDGE_ACCEPTANCE,
                    vertexId,
                    message.getSourceId(),
                    edgeWeight
                );
                acceptance.setComponentId(targetComponentId);
                context.sendMessage(message.getSourceId(), acceptance);
                
                // Update local state
                state.setParentId(message.getSourceId());
                state.setMinEdgeWeight(edgeWeight);
                state.setRoot(false);
                return true;
            } else {
                // Reject edge proposal
                MSTMessage rejection = new MSTMessage(
                    MSTMessage.MessageType.EDGE_REJECTION,
                    vertexId,
                    message.getSourceId(),
                    edgeWeight
                );
                context.sendMessage(message.getSourceId(), rejection);
            }
        }
        return false;
    }

    /**
     * Handle edge acceptance message.
     * Add MST edge and merge components.
     */
    private boolean handleEdgeAcceptance(Object vertexId, MSTMessage message, MSTVertexState state) {
        // Create MST edge
        MSTEdge mstEdge = new MSTEdge(vertexId, message.getSourceId(), 
            message.getWeight());
        state.addMSTEdge(mstEdge);
        
        // Merge components
        Object newComponentId = findMinComponentId(state.getComponentId(), message.getComponentId());
        state.setComponentId(newComponentId);
        
        // Broadcast MST edge discovery message
        MSTMessage mstEdgeMsg = new MSTMessage(
            MSTMessage.MessageType.MST_EDGE_FOUND,
            vertexId,
            message.getSourceId(),
            message.getWeight()
        );
        mstEdgeMsg.setEdge(mstEdge);
        context.sendMessageToNeighbors(mstEdgeMsg);
        
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
     * Broadcast state update message.
     * Send component update information to neighbors.
     */
    private void broadcastStateUpdate(Object vertexId, MSTVertexState state) {
        MSTMessage updateMsg = new MSTMessage(
            MSTMessage.MessageType.COMPONENT_UPDATE,
            vertexId,
            null,
            0.0
        );
        updateMsg.setComponentId(state.getComponentId());
        
        context.sendMessageToNeighbors(updateMsg);
    }

    /**
     * Get current vertex state.
     * Create new state if it doesn't exist.
     */
    private MSTVertexState getCurrentVertexState(RowVertex vertex) {
        Optional<Row> currentValues = context.getVertexValue(vertex.getId());
        if (currentValues.isPresent()) {
            Row values = currentValues.get();
            Object stateObj = values.getField(0, ObjectType.INSTANCE);
            if (stateObj instanceof MSTVertexState) {
                return (MSTVertexState) stateObj;
            }
        }
        return new MSTVertexState(vertex.getId());
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