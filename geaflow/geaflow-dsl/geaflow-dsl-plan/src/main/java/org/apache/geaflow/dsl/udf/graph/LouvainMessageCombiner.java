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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Message combiner for Louvain algorithm to reduce network traffic.
 *

 * <p>
 * This combiner merges multiple messages with the same community ID
 * into a single message with aggregated weights, significantly reducing
 * the number of messages transmitted across the network.
 * </p>
 */
public class LouvainMessageCombiner {

    /** Track aggregated weights for each community. */
    private Map<Object, Double> aggregatedWeights;

    /**
     * Default constructor.
     */
    public LouvainMessageCombiner() {
        this.aggregatedWeights = new HashMap<>();
    }

    /**
     * Combine multiple messages by aggregating their weights.
     *

     * @param messages Iterator of messages to combine.
     * @return A map of community IDs to aggregated weights.
     */
    public Map<Object, Double> combineMessages(Iterator<LouvainMessage> messages) {
        aggregatedWeights.clear();
        while (messages.hasNext()) {
            LouvainMessage msg = messages.next();
            if (msg.getMessageType() == LouvainMessage.MessageType.COMMUNITY_INFO) {
                aggregateCommunityWeight(msg.getCommunityId(), msg.getEdgeWeight());
            }
        }
        return aggregatedWeights;
    }

    /**
     * Combine compressed messages.
     *

     * @param messages Iterator of compressed messages to combine.
     * @return A map of community IDs to aggregated weights.
     */
    public Map<Object, Double> combineCompressedMessages(
            Iterator<LouvainCompressedMessage> messages) {
        aggregatedWeights.clear();
        while (messages.hasNext()) {
            LouvainCompressedMessage msg = messages.next();
            Map<Object, Double> weights = msg.getCommunityWeights();
            for (Map.Entry<Object, Double> entry : weights.entrySet()) {
                aggregateCommunityWeight(entry.getKey(), entry.getValue());
            }
        }
        return aggregatedWeights;
    }

    /**
     * Add or merge weight for a community.
     *

     * @param communityId The community ID.
     * @param weight The weight to aggregate.
     */
    private void aggregateCommunityWeight(Object communityId, double weight) {
        this.aggregatedWeights.put(communityId,
                this.aggregatedWeights.getOrDefault(communityId, 0.0) + weight);
    }

    /**
     * Get the aggregated weights.
     *

     * @return The aggregated weights map.
     */
    public Map<Object, Double> getAggregatedWeights() {
        return aggregatedWeights;
    }

    /**
     * Get the total aggregated weight.
     *

     * @return The sum of all aggregated weights.
     */
    public double getTotalWeight() {
        return aggregatedWeights.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    /**
     * Get the number of unique communities.
     *

     * @return The count of unique communities.
     */
    public int getCommunityCount() {
        return aggregatedWeights.size();
    }

    /**
     * Clear the aggregated weights.
     */
    public void clear() {
        aggregatedWeights.clear();
    }
}
