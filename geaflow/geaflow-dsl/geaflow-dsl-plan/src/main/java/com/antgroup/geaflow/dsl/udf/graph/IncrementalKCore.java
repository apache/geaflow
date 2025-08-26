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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 增量K-Core算法实现
 * 支持动态图的增量更新，处理边添加和删除的场景
 * 
 * 算法原理：
 * 1. 维护顶点的K-Core值缓存和度数信息
 * 2. 当图发生变化时，只重新计算受影响的顶点
 * 3. 通过消息传递机制在邻居间传播重计算请求
 * 4. 实现智能剪枝，避免不必要的重计算
 * 
 * @author TuGraph Analytics Team
 */
@Description(name = "incremental_kcore", description = "built-in udga for Incremental K-Core")
public class IncrementalKCore implements AlgorithmUserFunction<Object, IncrementalKCore.KCoreMessage>,
    IncrementalAlgorithmUserFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalKCore.class);
    
    private AlgorithmRuntimeContext<Object, KCoreMessage> context;
    private int k = 3; // 默认K值
    private int maxIterations = 100; // 最大迭代次数
    private double convergenceThreshold = 0.001; // 收敛阈值
    
    // 顶点状态缓存
    private Map<Object, Integer> vertexCoreValues = new HashMap<>();
    private Map<Object, Integer> vertexDegrees = new HashMap<>();
    private Set<Object> affectedVertices = new HashSet<>();
    private boolean hasConverged = false;
    
    @Override
    public void init(AlgorithmRuntimeContext<Object, KCoreMessage> context, Object[] parameters) {
        this.context = context;
        
        // 解析参数：k, maxIterations, convergenceThreshold
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
        
        Object vertexId = vertex.getId();
        long currentIteration = context.getCurrentIterationId();
        
        // 第一次迭代：初始化
        if (currentIteration == 1L) {
            initializeVertex(vertex);
            return;
        }
        
        // 检查是否已收敛或达到最大迭代次数
        if (hasConverged || currentIteration > maxIterations) {
            return;
        }
        
        // 处理增量更新
        processIncrementalUpdate(vertex, messages);
    }
    
    /**
     * 初始化顶点状态
     * 计算初始度数和K-Core值
     */
    private void initializeVertex(RowVertex vertex) {
        Object vertexId = vertex.getId();
        
        // 计算初始度数（包括静态边和动态边）
        List<RowEdge> staticEdges = context.loadStaticEdges(EdgeDirection.BOTH);
        List<RowEdge> dynamicEdges = context.loadDynamicEdges(EdgeDirection.BOTH);
        
        int totalDegree = staticEdges.size() + dynamicEdges.size();
        vertexDegrees.put(vertexId, totalDegree);
        
        // 初始K-Core值设为度数
        int initialCore = totalDegree;
        vertexCoreValues.put(vertexId, initialCore);
        
        // 更新顶点值：core_value, degree, change_status
        context.updateVertexValue(ObjectRow.create(initialCore, totalDegree, "INIT"));
        
        // 向邻居发送初始化消息
        List<RowEdge> allEdges = new ArrayList<>();
        allEdges.addAll(staticEdges);
        allEdges.addAll(dynamicEdges);
        
        sendMessageToNeighbors(allEdges, new KCoreMessage(vertexId, initialCore, KCoreMessage.MessageType.INIT));
        
        LOGGER.debug("Initialized vertex {} with degree={}, core={}", vertexId, totalDegree, initialCore);
    }
    
    /**
     * 处理增量更新消息
     * 根据接收到的消息类型进行相应的处理
     */
    private void processIncrementalUpdate(RowVertex vertex, Iterator<KCoreMessage> messages) {
        Object vertexId = vertex.getId();
        boolean needsRecomputation = false;
        Set<Object> changedNeighbors = new HashSet<>();
        Map<Object, Integer> neighborCores = new HashMap<>();
        
        // 处理接收到的消息
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
                    needsRecomputation = true;
                    break;
            }
        }
        
        if (needsRecomputation) {
            recomputeKCore(vertex, neighborCores);
        }
    }
    
    /**
     * 处理边添加事件
     * 增加顶点的度数并标记为受影响顶点
     */
    private void handleEdgeAdded(Object vertexId, KCoreMessage message) {
        // 增加度数
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);
        vertexDegrees.put(vertexId, currentDegree + 1);
        affectedVertices.add(vertexId);
        
        LOGGER.debug("Edge added to vertex {}, new degree={}", vertexId, currentDegree + 1);
    }
    
    /**
     * 处理边删除事件
     * 减少顶点的度数并标记为受影响顶点
     */
    private void handleEdgeRemoved(Object vertexId, KCoreMessage message) {
        // 减少度数
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);
        vertexDegrees.put(vertexId, Math.max(0, currentDegree - 1));
        affectedVertices.add(vertexId);
        
        LOGGER.debug("Edge removed from vertex {}, new degree={}", vertexId, currentDegree - 1);
    }
    
    /**
     * 重新计算K-Core值
     * 基于当前邻居的Core值和度数重新计算
     */
    private void recomputeKCore(RowVertex vertex, Map<Object, Integer> neighborCores) {
        Object vertexId = vertex.getId();
        int currentCore = vertexCoreValues.getOrDefault(vertexId, 0);
        
        // 计算有效邻居数（K-Core值 >= k的邻居）
        int validNeighbors = countValidNeighbors(vertex, neighborCores);
        
        // 重新计算K-Core值：取有效邻居数和当前度数的最小值
        int currentDegree = vertexDegrees.getOrDefault(vertexId, 0);
        int newCore = Math.min(validNeighbors, currentDegree);
        
        // 如果新的core值小于k，则该顶点不属于k-core
        if (newCore < k) {
            newCore = 0;
        }
        
        // 如果Core值发生变化，更新状态并广播变化
        if (newCore != currentCore) {
            vertexCoreValues.put(vertexId, newCore);
            affectedVertices.add(vertexId);
            
            // 更新顶点值
            String changeType = newCore > currentCore ? "INCREASED" : "DECREASED";
            context.updateVertexValue(ObjectRow.create(newCore, currentDegree, changeType));
            
            // 向邻居广播变化
            List<RowEdge> allEdges = new ArrayList<>();
            allEdges.addAll(context.loadStaticEdges(EdgeDirection.BOTH));
            allEdges.addAll(context.loadDynamicEdges(EdgeDirection.BOTH));
            
            sendMessageToNeighbors(allEdges, 
                new KCoreMessage(vertexId, newCore, KCoreMessage.MessageType.CORE_CHANGED));
            
            LOGGER.debug("Vertex {} core changed from {} to {}", vertexId, currentCore, newCore);
        }
    }
    
    /**
     * 计算有效邻居数
     * 统计K-Core值 >= k的邻居数量
     */
    private int countValidNeighbors(RowVertex vertex, Map<Object, Integer> neighborCores) {
        List<RowEdge> allEdges = new ArrayList<>();
        allEdges.addAll(context.loadStaticEdges(EdgeDirection.BOTH));
        allEdges.addAll(context.loadDynamicEdges(EdgeDirection.BOTH));
        
        int validCount = 0;
        for (RowEdge edge : allEdges) {
            Object neighborId = edge.getTargetId().equals(vertex.getId()) ? 
                edge.getSrcId() : edge.getTargetId();
            
            int neighborCore = neighborCores.getOrDefault(neighborId, 
                vertexCoreValues.getOrDefault(neighborId, 0));
            
            if (neighborCore >= k) {
                validCount++;
            }
        }
        
        return validCount;
    }
    
    /**
     * 向邻居发送消息
     * 遍历所有边，向邻居顶点发送消息
     */
    private void sendMessageToNeighbors(List<RowEdge> edges, KCoreMessage message) {
        for (RowEdge edge : edges) {
            Object targetId = edge.getTargetId();
            Object srcId = edge.getSrcId();
            
            // 发送给源顶点和目标顶点（如果不是消息发送者）
            if (!targetId.equals(message.getSourceId())) {
                context.sendMessage(targetId, message);
            }
            if (!srcId.equals(message.getSourceId()) && !srcId.equals(targetId)) {
                context.sendMessage(srcId, message);
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(vertex::setValue);
        
        Object vertexId = vertex.getId();
        int coreValue = vertexCoreValues.getOrDefault(vertexId, 0);
        int degree = vertexDegrees.getOrDefault(vertexId, 0);
        
        // 只输出属于k-core的顶点
        if (coreValue >= k) {
            String changeStatus = affectedVertices.contains(vertexId) ? "CHANGED" : "UNCHANGED";
            context.take(ObjectRow.create(vertexId, coreValue, degree, changeStatus));
        }
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
     * K-Core消息类
     * 用于顶点间通信的消息类型
     */
    public static class KCoreMessage implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        /** 消息类型枚举 */
        public enum MessageType {
            INIT,           // 初始化消息
            EDGE_ADDED,     // 边添加消息
            EDGE_REMOVED,   // 边删除消息
            CORE_CHANGED    // Core值变化消息
        }
        
        private Object sourceId;
        private int coreValue;
        private MessageType type;
        private Object edgeInfo; // 可选的边信息
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
            return "KCoreMessage{" +
                "sourceId=" + sourceId +
                ", coreValue=" + coreValue +
                ", type=" + type +
                ", edgeInfo=" + edgeInfo +
                ", timestamp=" + timestamp +
                '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            KCoreMessage that = (KCoreMessage) obj;
            return coreValue == that.coreValue &&
                   timestamp == that.timestamp &&
                   Objects.equals(sourceId, that.sourceId) &&
                   type == that.type &&
                   Objects.equals(edgeInfo, that.edgeInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceId, coreValue, type, edgeInfo, timestamp);
        }
    }
}

import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
