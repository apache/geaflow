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
import java.util.HashMap;
import java.util.Map;

/**
 * Vertex value class for Louvain community detection algorithm.
 * Maintains the community information and weight statistics for each vertex.
 */
public class LouvainVertexValue implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Current community ID that this vertex belongs to. */
    private Object communityId;

    /** Total weight (degree) of edges connected to this vertex. */
    private double totalWeight;

    /** Weight of edges within the same community. */
    private double internalWeight;

    /** Mapping of neighbor community IDs to the weight between this vertex and that community. */
    private Map<Object, Double> neighborCommunityWeights;

    /**
     * Default constructor.
     */
    public LouvainVertexValue() {
        this.neighborCommunityWeights = new HashMap<>();
    }

    /**
     * Constructor with community ID and initial weights.
     *
     * @param communityId    The initial community ID (typically vertex ID).
     * @param totalWeight    The total weight of connected edges.
     * @param internalWeight The weight of internal edges.
     */
    public LouvainVertexValue(Object communityId, double totalWeight, double internalWeight) {
        this.communityId = communityId;
        this.totalWeight = totalWeight;
        this.internalWeight = internalWeight;
        this.neighborCommunityWeights = new HashMap<>();
    }

    /**
     * Update the weight to a neighbor community.
     *
     * @param communityId The neighbor community ID.
     * @param weight      The edge weight.
     */
    public void addNeighborCommunityWeight(Object communityId, double weight) {
        this.neighborCommunityWeights.put(communityId,
                this.neighborCommunityWeights.getOrDefault(communityId, 0.0) + weight);
    }

    /**
     * Clear all neighbor community weights.
     */
    public void clearNeighborCommunityWeights() {
        this.neighborCommunityWeights.clear();
    }

    public Object getCommunityId() {
        return communityId;
    }

    public void setCommunityId(Object communityId) {
        this.communityId = communityId;
    }

    public double getTotalWeight() {
        return totalWeight;
    }

    public void setTotalWeight(double totalWeight) {
        this.totalWeight = totalWeight;
    }

    public double getInternalWeight() {
        return internalWeight;
    }

    public void setInternalWeight(double internalWeight) {
        this.internalWeight = internalWeight;
    }

    public Map<Object, Double> getNeighborCommunityWeights() {
        return neighborCommunityWeights;
    }

    public void setNeighborCommunityWeights(Map<Object, Double> neighborCommunityWeights) {
        this.neighborCommunityWeights = neighborCommunityWeights;
    }

    @Override
    public String toString() {
        return "LouvainVertexValue{"
                + "communityId=" + communityId
                + ", totalWeight=" + totalWeight
                + ", internalWeight=" + internalWeight
                + ", neighborCommunityWeights=" + neighborCommunityWeights
                + '}';
    }
}
