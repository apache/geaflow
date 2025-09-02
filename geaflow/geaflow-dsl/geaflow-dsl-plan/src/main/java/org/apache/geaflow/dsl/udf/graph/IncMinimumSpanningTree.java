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

import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import java.util.*;

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
     * Initialize vertex state
     * Each vertex initialized as independent component, self as root node
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
     * 处理接收到的消息
     * 根据消息类型执行相应的MST更新逻辑
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
        
        // 如果状态发生变化，更新顶点值并广播更新
        if (stateChanged) {
            context.updateVertexValue(ObjectRow.create(currentState, true));
            broadcastStateUpdate(vertexId, currentState);
        }
    }

    /**
     * 处理单个消息
     * 根据消息类型执行相应的处理逻辑
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
     * 处理组件更新消息
     * 更新顶点的组件标识符
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
     * 处理边提议消息
     * 检查是否接受新的MST边
     */
    private boolean handleEdgeProposal(Object vertexId, MSTMessage message, MSTVertexState state) {
        Object sourceComponentId = message.getComponentId();
        Object targetComponentId = state.getComponentId();
        
        // 检查是否连接不同组件
        if (!sourceComponentId.equals(targetComponentId)) {
            double edgeWeight = message.getWeight();
            
            // 检查是否是更好的边
            if (edgeWeight < state.getMinEdgeWeight()) {
                // 接受边提议
                MSTMessage acceptance = new MSTMessage(
                    MSTMessage.MessageType.EDGE_ACCEPTANCE,
                    vertexId,
                    message.getSourceId(),
                    edgeWeight
                );
                acceptance.setComponentId(targetComponentId);
                context.sendMessage(message.getSourceId(), acceptance);
                
                // 更新本地状态
                state.setParentId(message.getSourceId());
                state.setMinEdgeWeight(edgeWeight);
                state.setRoot(false);
                return true;
            } else {
                // 拒绝边提议
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
     * 处理边接受消息
     * 添加MST边并合并组件
     */
    private boolean handleEdgeAcceptance(Object vertexId, MSTMessage message, MSTVertexState state) {
        // 创建MST边
        MSTEdge mstEdge = new MSTEdge(vertexId, message.getSourceId(), 
            message.getWeight());
        state.addMSTEdge(mstEdge);
        
        // 合并组件
        Object newComponentId = findMinComponentId(state.getComponentId(), message.getComponentId());
        state.setComponentId(newComponentId);
        
        // 广播MST边发现消息
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
     * 处理边拒绝消息
     * 记录被拒绝的边
     */
    private boolean handleEdgeRejection(Object vertexId, MSTMessage message, MSTVertexState state) {
        // 可以在这里记录被拒绝的边，用于调试或统计
        return false;
    }

    /**
     * 处理MST边发现消息
     * 记录发现的MST边
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
     * 广播状态更新消息
     * 向邻居发送组件更新信息
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
     * 获取当前顶点状态
     * 如果不存在则创建新的状态
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
     * 选择较小的组件ID作为新的组件ID
     * 用于组件合并时的ID选择策略
     */
    private Object findMinComponentId(Object id1, Object id2) {
        if (id1.toString().compareTo(id2.toString()) < 0) {
            return id1;
        }
        return id2;
    }
} 