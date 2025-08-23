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
 * MST边类
 * 表示最小生成树中的边
 * 
 * 包含信息：
 * - sourceId: 源顶点ID
 * - targetId: 目标顶点ID
 * - weight: 边权重
 * 
 * @author TuGraph Analytics Team
 */
public class MSTEdge implements Serializable, Comparable<MSTEdge> {
    
    private static final long serialVersionUID = 1L;
    
    /** 源顶点ID */
    private Object sourceId;
    
    /** 目标顶点ID */
    private Object targetId;
    
    /** 边权重 */
    private double weight;

    /**
     * 构造函数
     * @param sourceId 源顶点ID
     * @param targetId 目标顶点ID
     * @param weight 边权重
     */
    public MSTEdge(Object sourceId, Object targetId, double weight) {
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.weight = weight;
    }

    // Getters and Setters
    
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

    /**
     * 获取边的另一个端点
     * @param vertexId 已知的顶点ID
     * @return 另一个端点ID，如果vertexId不是边的端点则返回null
     */
    public Object getOtherEndpoint(Object vertexId) {
        if (sourceId.equals(vertexId)) {
            return targetId;
        } else if (targetId.equals(vertexId)) {
            return sourceId;
        }
        return null;
    }

    /**
     * 检查指定顶点是否为边的端点
     * @param vertexId 顶点ID
     * @return 是否为端点
     */
    public boolean isEndpoint(Object vertexId) {
        return sourceId.equals(vertexId) || targetId.equals(vertexId);
    }

    /**
     * 检查是否为自环边
     * @return 是否为自环
     */
    public boolean isSelfLoop() {
        return sourceId.equals(targetId);
    }

    /**
     * 创建边的反向边
     * @return 反向边
     */
    public MSTEdge reverse() {
        return new MSTEdge(targetId, sourceId, weight);
    }

    /**
     * 检查两条边是否相等（忽略方向）
     * @param other 另一条边
     * @return 是否相等
     */
    public boolean equalsIgnoreDirection(MSTEdge other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        
        // 检查正向和反向是否相等
        boolean forwardEqual = Objects.equals(sourceId, other.sourceId) && 
                              Objects.equals(targetId, other.targetId);
        boolean reverseEqual = Objects.equals(sourceId, other.targetId) && 
                              Objects.equals(targetId, other.sourceId);
        
        return (forwardEqual || reverseEqual) && Double.compare(other.weight, weight) == 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MSTEdge edge = (MSTEdge) obj;
        return Double.compare(edge.weight, weight) == 0 &&
               Objects.equals(sourceId, edge.sourceId) &&
               Objects.equals(targetId, edge.targetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, targetId, weight);
    }

    @Override
    public int compareTo(MSTEdge other) {
        // 首先按权重比较
        int weightCompare = Double.compare(this.weight, other.weight);
        if (weightCompare != 0) {
            return weightCompare;
        }
        
        // 权重相同时，按源顶点ID比较
        int sourceCompare = sourceId.toString().compareTo(other.sourceId.toString());
        if (sourceCompare != 0) {
            return sourceCompare;
        }
        
        // 源顶点ID相同时，按目标顶点ID比较
        return targetId.toString().compareTo(other.targetId.toString());
    }

    @Override
    public String toString() {
        return "MSTEdge{" +
                "sourceId=" + sourceId +
                ", targetId=" + targetId +
                ", weight=" + weight +
                '}';
    }
} 