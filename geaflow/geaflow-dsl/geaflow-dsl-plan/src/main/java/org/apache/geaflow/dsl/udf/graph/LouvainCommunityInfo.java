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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Community information tracker for Louvain algorithm phase 2.
 *
 * <p>
 * This class tracks global statistics about communities for the aggregation phase,
 * including total weight within each community and edges between communities.
 * </p>
 */
public class LouvainCommunityInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Community ID. */
    private Object communityId;

    /** Set of member vertices in this community. */
    private Set<Object> memberVertices;

    /** Total internal weight (sum of edges within this community). */
    private double internalWeight;

    /** Total weight of all edges connected to this community. */
    private double totalWeight;

    /** Mapping of other communities to edge weights between them. */
    private Map<Object, Double> externalWeights;

    /** Modularity contribution of this community. */
    private double modularityContribution;

    /**
     * Default constructor.
     */
    public LouvainCommunityInfo() {
        this.memberVertices = new HashSet<>();
        this.externalWeights = new HashMap<>();
    }

    /**
     * Constructor with community ID.
     *

     * @param communityId The community ID.
     */
    public LouvainCommunityInfo(Object communityId) {
        this();
        this.communityId = communityId;
    }

    /**
     * Add a member vertex to this community.
     *

     * @param vertexId The vertex ID.
     */
    public void addMemberVertex(Object vertexId) {
        this.memberVertices.add(vertexId);
    }

    /**
     * Add internal weight (edges within the community).
     *

     * @param weight The weight to add.
     */
    public void addInternalWeight(double weight) {
        this.internalWeight += weight;
    }

    /**
     * Add total weight (all connected edges).
     *

     * @param weight The weight to add.
     */
    public void addTotalWeight(double weight) {
        this.totalWeight += weight;
    }

    /**
     * Add external weight to another community.
     *

     * @param otherCommunity The other community ID.
     * @param weight The weight.
     */
    public void addExternalWeight(Object otherCommunity, double weight) {
        this.externalWeights.put(otherCommunity,
                this.externalWeights.getOrDefault(otherCommunity, 0.0) + weight);
    }

    /**
     * Merge another community info into this one.
     *

     * @param other The other community info to merge.
     */
    public void merge(LouvainCommunityInfo other) {
        if (other == null) {
            return;
        }
        this.memberVertices.addAll(other.memberVertices);
        this.internalWeight += other.internalWeight;
        this.totalWeight += other.totalWeight;
        for (Map.Entry<Object, Double> entry : other.externalWeights.entrySet()) {
            addExternalWeight(entry.getKey(), entry.getValue());
        }
        this.modularityContribution += other.modularityContribution;
    }

    /**
     * Get community ID.
     *

     * @return The community ID.
     */
    public Object getCommunityId() {
        return communityId;
    }

    /**
     * Set community ID.
     *

     * @param communityId The community ID.
     */
    public void setCommunityId(Object communityId) {
        this.communityId = communityId;
    }

    /**
     * Get member vertices.
     *

     * @return The set of member vertices.
     */
    public Set<Object> getMemberVertices() {
        return memberVertices;
    }

    /**
     * Get internal weight.
     *

     * @return The internal weight.
     */
    public double getInternalWeight() {
        return internalWeight;
    }

    /**
     * Get total weight.
     *

     * @return The total weight.
     */
    public double getTotalWeight() {
        return totalWeight;
    }

    /**
     * Get external weights mapping.
     *

     * @return The external weights map.
     */
    public Map<Object, Double> getExternalWeights() {
        return externalWeights;
    }

    /**
     * Get modularity contribution.
     *

     * @return The modularity contribution.
     */
    public double getModularityContribution() {
        return modularityContribution;
    }

    /**
     * Set modularity contribution.
     *

     * @param modularityContribution The value to set.
     */
    public void setModularityContribution(double modularityContribution) {
        this.modularityContribution = modularityContribution;
    }

    /**
     * Get community size (number of members).
     *

     * @return The size.
     */
    public int getSize() {
        return memberVertices.size();
    }

    @Override
    public String toString() {
        return "LouvainCommunityInfo{"
                + "communityId=" + communityId
                + ", members=" + memberVertices.size()
                + ", internalWeight=" + internalWeight
                + ", totalWeight=" + totalWeight
                + '}';
    }
}
