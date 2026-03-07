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
 * Compressed message for Louvain algorithm that aggregates multiple weights.
 *
 * <p>
 * This message type compresses multiple edge weights to the same community
 * into a single message, reducing message count and network traffic.
 * </p>
 */
public class LouvainCompressedMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Mapping of community IDs to aggregated edge weights. */
    private Map<Object, Double> communityWeights;

    /** Source vertex ID (optional, for debugging). */
    private Object sourceVertexId;

    /**
     * Default constructor for deserialization.
     */
    public LouvainCompressedMessage() {
        this.communityWeights = new HashMap<>();
    }

    /**
     * Constructor with initial community weights.
     *
     * @param communityWeights The mapping of communities to weights.
     */
    public LouvainCompressedMessage(Map<Object, Double> communityWeights) {
        this.communityWeights = new HashMap<>(communityWeights);
    }

    /**
     * Constructor with source vertex ID.
     *
     * @param sourceVertexId The source vertex ID.
     * @param communityWeights The mapping of communities to weights.
     */
    public LouvainCompressedMessage(Object sourceVertexId,
                                    Map<Object, Double> communityWeights) {
        this.sourceVertexId = sourceVertexId;
        this.communityWeights = new HashMap<>(communityWeights);
    }

    /**
     * Add or merge weight for a community.
     *
     * @param communityId The community ID.
     * @param weight The weight to add.
     */
    public void addCommunityWeight(Object communityId, double weight) {
        this.communityWeights.put(communityId,
                this.communityWeights.getOrDefault(communityId, 0.0) + weight);
    }

    /**
     * Merge another compressed message into this one.
     *
     * @param other The other message to merge.
     */
    public void merge(LouvainCompressedMessage other) {
        if (other != null && other.communityWeights != null) {
            for (Map.Entry<Object, Double> entry : other.communityWeights.entrySet()) {
                addCommunityWeight(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Get community weights mapping.
     *
     * @return The community weights map.
     */
    public Map<Object, Double> getCommunityWeights() {
        return communityWeights;
    }

    /**
     * Set community weights mapping.
     *
     * @param communityWeights The weights map to set.
     */
    public void setCommunityWeights(Map<Object, Double> communityWeights) {
        this.communityWeights = communityWeights != null ? new HashMap<>(communityWeights)
                : new HashMap<>();
    }

    /**
     * Get source vertex ID.
     *
     * @return The source vertex ID.
     */
    public Object getSourceVertexId() {
        return sourceVertexId;
    }

    /**
     * Set source vertex ID.
     *
     * @param sourceVertexId The source vertex ID.
     */
    public void setSourceVertexId(Object sourceVertexId) {
        this.sourceVertexId = sourceVertexId;
    }

    /**
     * Get total weight in this message.
     *

     * @return The sum of all weights.
     */
    public double getTotalWeight() {
        return communityWeights.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    /**
     * Get number of unique communities in this message.
     *

     * @return The community count.
     */
    public int getCommunityCount() {
        return communityWeights.size();
    }

    @Override
    public String toString() {
        return "LouvainCompressedMessage{"
                + "sourceVertexId=" + sourceVertexId
                + ", communityWeights=" + communityWeights
                + '}';
    }
}
