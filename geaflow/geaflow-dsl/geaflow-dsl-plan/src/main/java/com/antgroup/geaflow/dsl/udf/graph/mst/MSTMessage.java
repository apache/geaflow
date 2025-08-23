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

package com.antgroup.geaflow.dsl.udf.graph.mst;

import java.io.Serializable;
import java.util.Objects;

/**
 * MST消息类
 * 用于顶点间的消息传递，支持不同类型的MST操作
 * 
 * 消息类型：
 * - COMPONENT_UPDATE: 组件更新消息
 * - EDGE_PROPOSAL: 边提议消息
 * - EDGE_ACCEPTANCE: 边接受消息
 * - EDGE_REJECTION: 边拒绝消息
 * - MST_EDGE_FOUND: MST边发现消息
 * 
 * @author TuGraph Analytics Team
 */
public class MSTMessage implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** 消息类型枚举 */
    public enum MessageType {
        /** 组件更新消息 */
        COMPONENT_UPDATE,
        /** 边提议消息 */
        EDGE_PROPOSAL,
        /** 边接受消息 */
        EDGE_ACCEPTANCE,
        /** 边拒绝消息 */
        EDGE_REJECTION,
        /** MST边发现消息 */
        MST_EDGE_FOUND
    }

    /** 消息类型 */
    private MessageType type;
    
    /** 源顶点ID */
    private Object sourceId;
    
    /** 目标顶点ID */
    private Object targetId;
    
    /** 边权重 */
    private double weight;
    
    /** 组件ID */
    private Object componentId;
    
    /** MST边 */
    private MSTEdge edge;
    
    /** 消息时间戳 */
    private long timestamp;

    /**
     * 构造函数
     * @param type 消息类型
     * @param sourceId 源顶点ID
     * @param targetId 目标顶点ID
     * @param weight 边权重
     */
    public MSTMessage(MessageType type, Object sourceId, Object targetId, double weight) {
        this.type = type;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.weight = weight;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 构造函数（带组件ID）
     * @param type 消息类型
     * @param sourceId 源顶点ID
     * @param targetId 目标顶点ID
     * @param weight 边权重
     * @param componentId 组件ID
     */
    public MSTMessage(MessageType type, Object sourceId, Object targetId, double weight, Object componentId) {
        this(type, sourceId, targetId, weight);
        this.componentId = componentId;
    }

    // Getters and Setters
    
    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public Object getSourceId() {
        return sourceId;
    }

    public void setSourceId(Object sourceId) {
        this.sourceId = sourceId;
    }

    public Object getTargetId() {
        return targetId;
    }

    public void setTargetId(Object targetId) {
        this.targetId = targetId;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Object getComponentId() {
        return componentId;
    }

    public void setComponentId(Object componentId) {
        this.componentId = componentId;
    }

    public MSTEdge getEdge() {
        return edge;
    }

    public void setEdge(MSTEdge edge) {
        this.edge = edge;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * 检查是否为组件更新消息
     * @return 是否为组件更新消息
     */
    public boolean isComponentUpdate() {
        return type == MessageType.COMPONENT_UPDATE;
    }

    /**
     * 检查是否为边提议消息
     * @return 是否为边提议消息
     */
    public boolean isEdgeProposal() {
        return type == MessageType.EDGE_PROPOSAL;
    }

    /**
     * 检查是否为边接受消息
     * @return 是否为边接受消息
     */
    public boolean isEdgeAcceptance() {
        return type == MessageType.EDGE_ACCEPTANCE;
    }

    /**
     * 检查是否为边拒绝消息
     * @return 是否为边拒绝消息
     */
    public boolean isEdgeRejection() {
        return type == MessageType.EDGE_REJECTION;
    }

    /**
     * 检查是否为MST边发现消息
     * @return 是否为MST边发现消息
     */
    public boolean isMSTEdgeFound() {
        return type == MessageType.MST_EDGE_FOUND;
    }

    /**
     * 检查消息是否过期
     * @param currentTime 当前时间
     * @param timeout 超时时间（毫秒）
     * @return 是否过期
     */
    public boolean isExpired(long currentTime, long timeout) {
        return (currentTime - timestamp) > timeout;
    }

    /**
     * 创建消息的副本
     * @return 消息副本
     */
    public MSTMessage copy() {
        MSTMessage copy = new MSTMessage(type, sourceId, targetId, weight, componentId);
        copy.setEdge(edge);
        copy.setTimestamp(timestamp);
        return copy;
    }

    /**
     * 创建反向消息
     * @return 反向消息
     */
    public MSTMessage reverse() {
        MSTMessage reverse = new MSTMessage(type, targetId, sourceId, weight, componentId);
        reverse.setEdge(edge);
        reverse.setTimestamp(timestamp);
        return reverse;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MSTMessage message = (MSTMessage) obj;
        return Double.compare(message.weight, weight) == 0 &&
               timestamp == message.timestamp &&
               type == message.type &&
               Objects.equals(sourceId, message.sourceId) &&
               Objects.equals(targetId, message.targetId) &&
               Objects.equals(componentId, message.componentId) &&
               Objects.equals(edge, message.edge);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, sourceId, targetId, weight, componentId, edge, timestamp);
    }

    @Override
    public String toString() {
        return "MSTMessage{" +
                "type=" + type +
                ", sourceId=" + sourceId +
                ", targetId=" + targetId +
                ", weight=" + weight +
                ", componentId=" + componentId +
                ", edge=" + edge +
                ", timestamp=" + timestamp +
                '}';
    }
} 