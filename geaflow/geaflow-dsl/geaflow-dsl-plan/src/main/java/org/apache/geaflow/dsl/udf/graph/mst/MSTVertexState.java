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

package org.apache.geaflow.dsl.udf.graph.mst;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * MST vertex state class.
 * Maintains state information for each vertex in the minimum spanning tree.
 * 
 * <p>Contains information:
 * - parentId: Parent node ID in MST
 * - componentId: Component ID it belongs to
 * - minEdgeWeight: Edge weight to parent node
 * - isRoot: Whether it is a root node
 * - mstEdges: MST edge set
 * - changed: Whether the state has changed
 * 
 * @author Geaflow Team
 */
public class MSTVertexState implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** Parent node ID in MST. */
    private Object parentId;
    
    /** Component ID it belongs to. */
    private Object componentId;
    
    /** Edge weight to parent node. */
    private double minEdgeWeight;
    
    /** Whether it is a root node. */
    private boolean isRoot;
    
    /** MST edge set with size limit to prevent memory overflow. */
    private Set<MSTEdge> mstEdges;
    
    /** Maximum number of MST edges to store per vertex (memory optimization). */
    private static final int MAX_MST_EDGES_PER_VERTEX = 100; // Reduced from 1000 to prevent memory overflow
    
    /** Whether the state has changed. */
    private boolean changed;
    
    /** Vertex ID. */
    private Object vertexId;

    /**
     * Constructor.
     * @param vertexId Vertex ID
     */
    public MSTVertexState(Object vertexId) {
        this.vertexId = vertexId;
        this.parentId = vertexId; // Initially self as parent node
        this.componentId = vertexId; // Initially self as independent component
        this.minEdgeWeight = Double.MAX_VALUE; // Initial weight as infinity
        this.isRoot = true; // Initially as root node
        this.mstEdges = new HashSet<>(); // Initial MST edge set is empty
        this.changed = false; // Initial state unchanged
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
     * Add MST edge with memory optimization.
     * Prevents memory overflow by limiting the number of stored edges.
     * @param edge MST edge
     * @return Whether addition was successful
     */
    public boolean addMSTEdge(MSTEdge edge) {
        // Memory optimization: limit the number of MST edges per vertex
        if (this.mstEdges.size() >= MAX_MST_EDGES_PER_VERTEX) {
            // Remove the edge with highest weight to make room for new edge
            MSTEdge heaviestEdge = this.mstEdges.stream()
                .max(MSTEdge::compareTo)
                .orElse(null);
            if (heaviestEdge != null && edge.getWeight() < heaviestEdge.getWeight()) {
                this.mstEdges.remove(heaviestEdge);
            } else {
                // New edge is heavier than all existing edges, skip it
                return false;
            }
        }
        
        boolean added = this.mstEdges.add(edge);
        if (added) {
            this.changed = true;
        }
        return added;
    }

    /**
     * Remove MST edge.
     * @param edge MST edge
     * @return Whether removal was successful
     */
    public boolean removeMSTEdge(MSTEdge edge) {
        boolean removed = this.mstEdges.remove(edge);
        if (removed) {
            this.changed = true;
        }
        return removed;
    }

    /**
     * Check if contains the specified MST edge.
     * @param edge MST edge
     * @return Whether it contains the edge
     */
    public boolean containsMSTEdge(MSTEdge edge) {
        return this.mstEdges.contains(edge);
    }

    /**
     * Get the number of MST edges.
     * @return Number of edges
     */
    public int getMSTEdgeCount() {
        return this.mstEdges.size();
    }

    /**
     * Clear MST edge set.
     */
    public void clearMSTEdges() {
        if (!this.mstEdges.isEmpty()) {
            this.mstEdges.clear();
            this.changed = true;
        }
    }

    /**
     * Reset state change flag.
     */
    public void resetChanged() {
        this.changed = false;
    }
    
    /**
     * Memory optimization: compact MST edges by removing redundant edges.
     * Keeps only the most important edges to prevent memory overflow.
     */
    public void compactMSTEdges() {
        if (this.mstEdges.size() > MAX_MST_EDGES_PER_VERTEX) {
            // Convert to sorted list and keep only the lightest edges
            Set<MSTEdge> compactedEdges = this.mstEdges.stream()
                .sorted()
                .limit(MAX_MST_EDGES_PER_VERTEX)
                .collect(java.util.stream.Collectors.toSet());
            
            this.mstEdges.clear();
            this.mstEdges.addAll(compactedEdges);
            this.changed = true;
        }
    }
    
    /**
     * Get memory usage estimate for this vertex state.
     * @return Estimated memory usage in bytes
     */
    public long getMemoryUsageEstimate() {
        long baseSize = 8 * 8; // Object overhead + 8 fields
        long edgesSize = this.mstEdges.size() * 32; // Approximate size per MSTEdge
        return baseSize + edgesSize;
    }

    /**
     * Check if it is a leaf node (no child nodes).
     * @return Whether it is a leaf node
     */
    public boolean isLeaf() {
        return this.mstEdges.isEmpty();
    }

    /**
     * Get edge weight to specified vertex.
     * @param targetId Target vertex ID
     * @return Edge weight, returns Double.MAX_VALUE if not exists
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
     * Check if connected to specified vertex.
     * @param targetId Target vertex ID
     * @return Whether connected
     */
    public boolean isConnectedTo(Object targetId) {
        return getEdgeWeightTo(targetId) < Double.MAX_VALUE;
    }

    @Override
    public String toString() {
        return "MSTVertexState{"
                + "vertexId=" + vertexId
                + ", parentId=" + parentId
                + ", componentId=" + componentId
                + ", minEdgeWeight=" + minEdgeWeight
                + ", isRoot=" + isRoot
                + ", mstEdges=" + mstEdges
                + ", changed=" + changed
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
        MSTVertexState that = (MSTVertexState) obj;
        return Double.compare(that.minEdgeWeight, minEdgeWeight) == 0
            && isRoot == that.isRoot
            && changed == that.changed
            && Objects.equals(vertexId, that.vertexId)
            && Objects.equals(parentId, that.parentId)
            && Objects.equals(componentId, that.componentId)
            && Objects.equals(mstEdges, that.mstEdges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexId, parentId, componentId, minEdgeWeight, isRoot, mstEdges, changed);
    }
} 