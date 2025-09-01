/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may not use this file except in compliance
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

package org.apache.geaflow.dsl.udf.graph.mst;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.Objects;

/**
 * MST顶点状态类
 * 维护每个顶点在最小生成树中的状态信息
 * 
 * 包含信息：
 * - parentId: MST中的父节点ID
 * - componentId: 所属组件ID
 * - minEdgeWeight: 到父节点的边权重
 * - isRoot: 是否为根节点
 * - mstEdges: MST边集合
 * - changed: 状态是否发生变化
 * 
 * @author TuGraph Analytics Team
 */
public class MSTVertexState implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** MST中的父节点ID */
    private Object parentId;
    
    /** 所属组件ID */
    private Object componentId;
    
    /** 到父节点的边权重 */
    private double minEdgeWeight;
    
    /** 是否为根节点 */
    private boolean isRoot;
    
    /** MST边集合 */
    private Set<MSTEdge> mstEdges;
    
    /** 状态是否发生变化 */
    private boolean changed;
    
    /** 顶点ID */
    private Object vertexId;

    /**
     * 构造函数
     * @param vertexId 顶点ID
     */
    public MSTVertexState(Object vertexId) {
        this.vertexId = vertexId;
        this.parentId = vertexId; // 初始时自己为父节点
        this.componentId = vertexId; // 初始时自己为独立组件
        this.minEdgeWeight = Double.MAX_VALUE; // 初始权重为无穷大
        this.isRoot = true; // 初始时为根节点
        this.mstEdges = new HashSet<>(); // 初始MST边集为空
        this.changed = false; // 初始状态未变化
    }

    // Getters and Setters
    
    public Object getParentId() {
        return parentId;
    }

    public void setParentId(Object parentId) {
        this.parentId = parentId;
        this.changed = true;
    }

    public Object getComponentId() {
        return componentId;
    }

    public void setComponentId(Object componentId) {
        this.componentId = componentId;
        this.changed = true;
    }

    public double getMinEdgeWeight() {
        return minEdgeWeight;
    }

    public void setMinEdgeWeight(double minEdgeWeight) {
        this.minEdgeWeight = minEdgeWeight;
        this.changed = true;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        this.isRoot = root;
        this.changed = true;
    }

    public Set<MSTEdge> getMstEdges() {
        return mstEdges;
    }

    public void setMstEdges(Set<MSTEdge> mstEdges) {
        this.mstEdges = mstEdges;
        this.changed = true;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public Object getVertexId() {
        return vertexId;
    }

    public void setVertexId(Object vertexId) {
        this.vertexId = vertexId;
    }

    /**
     * 添加MST边
     * @param edge MST边
     */
    public void addMSTEdge(MSTEdge edge) {
        if (this.mstEdges.add(edge)) {
            this.changed = true;
        }
    }

    /**
     * 移除MST边
     * @param edge MST边
     * @return 是否成功移除
     */
    public boolean removeMSTEdge(MSTEdge edge) {
        boolean removed = this.mstEdges.remove(edge);
        if (removed) {
            this.changed = true;
        }
        return removed;
    }

    /**
     * 检查是否包含指定的MST边
     * @param edge MST边
     * @return 是否包含
     */
    public boolean containsMSTEdge(MSTEdge edge) {
        return this.mstEdges.contains(edge);
    }

    /**
     * 获取MST边的数量
     * @return 边数量
     */
    public int getMSTEdgeCount() {
        return this.mstEdges.size();
    }

    /**
     * 清空MST边集合
     */
    public void clearMSTEdges() {
        if (!this.mstEdges.isEmpty()) {
            this.mstEdges.clear();
            this.changed = true;
        }
    }

    /**
     * 重置状态变化标志
     */
    public void resetChanged() {
        this.changed = false;
    }

    /**
     * 检查是否为叶子节点（没有子节点）
     * @return 是否为叶子节点
     */
    public boolean isLeaf() {
        return this.mstEdges.isEmpty();
    }

    /**
     * 获取到指定顶点的边权重
     * @param targetId 目标顶点ID
     * @return 边权重，如果不存在则返回Double.MAX_VALUE
     */
    public double getEdgeWeightTo(Object targetId) {
        for (MSTEdge edge : mstEdges) {
            if (edge.getTargetId().equals(targetId) || edge.getSourceId().equals(targetId)) {
                return edge.getWeight();
            }
        }
        return Double.MAX_VALUE;
    }

    /**
     * 检查是否与指定顶点相连
     * @param targetId 目标顶点ID
     * @return 是否相连
     */
    public boolean isConnectedTo(Object targetId) {
        return getEdgeWeightTo(targetId) < Double.MAX_VALUE;
    }

    @Override
    public String toString() {
        return "MSTVertexState{" +
                "vertexId=" + vertexId +
                ", parentId=" + parentId +
                ", componentId=" + componentId +
                ", minEdgeWeight=" + minEdgeWeight +
                ", isRoot=" + isRoot +
                ", mstEdges=" + mstEdges +
                ", changed=" + changed +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MSTVertexState that = (MSTVertexState) obj;
        return Double.compare(that.minEdgeWeight, minEdgeWeight) == 0 &&
                isRoot == that.isRoot &&
                changed == that.changed &&
                Objects.equals(vertexId, that.vertexId) &&
                Objects.equals(parentId, that.parentId) &&
                Objects.equals(componentId, that.componentId) &&
                Objects.equals(mstEdges, that.mstEdges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexId, parentId, componentId, minEdgeWeight, isRoot, mstEdges, changed);
    }
} 