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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Community aggregator for Louvain algorithm: community aggregation and graph reconstruction.
 *
 * <p>
 * This class handles community aggregation where:
 * 1. Communities are contracted into super-nodes
 * 2. Edges between communities are recalculated
 * 3. A new graph is created for the next iteration
 * </p>
 */
public class LouvainAggregator {

    /** Community information map: communityId -> LouvainCommunityInfo. */
    private Map<Object, LouvainCommunityInfo> communityMap;

    /** Edge map for the new contracted graph: (community1, community2) -> weight. */
    private Map<String, Double> newEdgeWeights;

    /** Super-node vertices (one per community). */
    private List<Object> superNodes;

    /** Total weight of the original graph. */
    private double totalEdgeWeight;

    /**
     * Default constructor.
     */
    public LouvainAggregator() {
        this.communityMap = new HashMap<>();
        this.newEdgeWeights = new HashMap<>();
        this.superNodes = new ArrayList<>();
    }

    /**
     * Add or update community information from a vertex.
     *
     * @param vertexId The original vertex ID.
     * @param communityId The community ID this vertex belongs to.
     * @param vertexWeight The total weight of edges connected to this vertex.
     */
    public void addVertexToCommunity(Object vertexId, Object communityId, double vertexWeight) {
        LouvainCommunityInfo community = communityMap.computeIfAbsent(communityId,
                k -> new LouvainCommunityInfo(communityId));
        community.addMemberVertex(vertexId);
        community.addTotalWeight(vertexWeight);
    }

    /**
     * Record an edge weight between two communities.
     *
     * @param sourceVertexId The source vertex ID.
     * @param targetVertexId The target vertex ID.
     * @param sourceCommunity The source community ID.
     * @param targetCommunity The target community ID.
     * @param edgeWeight The weight of the edge.
     */
    public void addEdgeBetweenCommunities(Object sourceVertexId, Object targetVertexId,
                                          Object sourceCommunity, Object targetCommunity,
                                          double edgeWeight) {
        // Update external weights
        LouvainCommunityInfo sourceCom = communityMap.get(sourceCommunity);
        LouvainCommunityInfo targetCom = communityMap.get(targetCommunity);

        if (sourceCom != null) {
            sourceCom.addExternalWeight(targetCommunity, edgeWeight);
        }
        if (targetCom != null) {
            targetCom.addExternalWeight(sourceCommunity, edgeWeight);
        }

        // Create edge key for the contracted graph
        String edgeKey = createEdgeKey(sourceCommunity, targetCommunity);
        newEdgeWeights.put(edgeKey,
                newEdgeWeights.getOrDefault(edgeKey, 0.0) + edgeWeight);
    }

    /**
     * Mark internal edge within a community.
     *
     * @param communityId The community ID.
     * @param edgeWeight The weight of the internal edge.
     */
    public void addInternalEdge(Object communityId, double edgeWeight) {
        LouvainCommunityInfo community = communityMap.get(communityId);
        if (community != null) {
            community.addInternalWeight(edgeWeight);
        }
    }

    /**
     * Finalize and create super-nodes.
     *
     * @return List of super-node IDs (community IDs).
     */
    public List<Object> finalizeSuperNodes() {
        superNodes.clear();
        superNodes.addAll(communityMap.keySet());
        return superNodes;
    }

    /**
     * Get contracted edges for the new graph.
     *
     * @return Map of edge keys to their weights.
     */
    public Map<String, Double> getNewEdgeWeights() {
        return newEdgeWeights;
    }

    /**
     * Get community information.
     *
     * @return Map of community IDs to their info.
     */
    public Map<Object, LouvainCommunityInfo> getCommunityMap() {
        return communityMap;
    }

    /**
     * Calculate modularity contribution for each community.
     *
     * @return Map of community IDs to their modularity contribution.
     */
    public Map<Object, Double> calculateModularityContributions() {
        Map<Object, Double> contributions = new HashMap<>();

        if (totalEdgeWeight == 0) {
            return contributions;
        }

        for (LouvainCommunityInfo community : communityMap.values()) {
            double a = community.getInternalWeight() / totalEdgeWeight;
            double b = community.getTotalWeight() / (2 * totalEdgeWeight);
            double contribution = a - (b * b);
            contributions.put(community.getCommunityId(), contribution);
        }

        return contributions;
    }

    /**
     * Get total modularity.
     *
     * @param totalWeight The total edge weight in the original graph.
     * @return The total modularity.
     */
    public double getTotalModularity(double totalWeight) {
        this.totalEdgeWeight = totalWeight;
        double modularity = 0.0;
        for (LouvainCommunityInfo community : communityMap.values()) {
            double a = community.getInternalWeight() / totalWeight;
            double b = community.getTotalWeight() / (2 * totalWeight);
            modularity += a - (b * b);
        }
        return modularity;
    }

    /**
     * Get the number of communities.
     *
     * @return Community count.
     */
    public int getCommunityCount() {
        return communityMap.size();
    }

    /**
     * Get statistics of a community.
     *
     * @param communityId The community ID.
     * @return The community info or null.
     */
    public LouvainCommunityInfo getCommunityInfo(Object communityId) {
        return communityMap.get(communityId);
    }

    /**
     * Create a unique edge key for two communities.
     *
     * @param community1 First community ID.
     * @param community2 Second community ID.
     * @return The edge key.
     */
    private String createEdgeKey(Object community1, Object community2) {
        // Ensure consistent ordering: smaller ID first
        int cmp = community1.toString().compareTo(community2.toString());
        if (cmp < 0) {
            return community1 + "-" + community2;
        } else if (cmp > 0) {
            return community2 + "-" + community1;
        } else {
            // Same community (internal edge)
            return community1 + "-" + community1;
        }
    }

    /**
     * Reset the aggregator for a new iteration.
     */
    public void reset() {
        communityMap.clear();
        newEdgeWeights.clear();
        superNodes.clear();
        totalEdgeWeight = 0;
    }

    @Override
    public String toString() {
        return "LouvainAggregator{"
                + "communities=" + communityMap.size()
                + ", newEdges=" + newEdgeWeights.size()
                + ", totalWeight=" + totalEdgeWeight
                + '}';
    }
}
