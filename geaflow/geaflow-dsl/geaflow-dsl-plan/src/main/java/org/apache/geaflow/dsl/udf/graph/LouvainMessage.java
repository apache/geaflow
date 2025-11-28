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

/**
 * Message class for Louvain community detection algorithm.
 * Used to transmit community information between vertices during iterations.
 */
public class LouvainMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Community ID that the source vertex belongs to. */
    private Object communityId;

    /** Weight of the edge from source to target vertex. */
    private double edgeWeight;

    /** Message type: COMMUNITY_INFO or WEIGHT_UPDATE. */
    private MessageType messageType;

    /**
     * Enum for different message types in Louvain algorithm.
     */
    public enum MessageType {
        /** Message carrying community information. */
        COMMUNITY_INFO,
        /** Message carrying weight updates. */
        WEIGHT_UPDATE
    }

    /**
     * Default constructor for deserialization.
     */
    public LouvainMessage() {
    }

    /**
     * Constructor with community ID and edge weight.
     *
     * @param communityId The community ID.
     * @param edgeWeight  The edge weight.
     */
    public LouvainMessage(Object communityId, double edgeWeight) {
        this(communityId, edgeWeight, MessageType.COMMUNITY_INFO);
    }

    /**
     * Constructor with all parameters.
     *
     * @param communityId  The community ID.
     * @param edgeWeight   The edge weight.
     * @param messageType  The message type.
     */
    public LouvainMessage(Object communityId, double edgeWeight, MessageType messageType) {
        this.communityId = communityId;
        this.edgeWeight = edgeWeight;
        this.messageType = messageType;
    }

    public Object getCommunityId() {
        return communityId;
    }

    public void setCommunityId(Object communityId) {
        this.communityId = communityId;
    }

    public double getEdgeWeight() {
        return edgeWeight;
    }

    public void setEdgeWeight(double edgeWeight) {
        this.edgeWeight = edgeWeight;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    @Override
    public String toString() {
        return "LouvainMessage{"
                + "communityId=" + communityId
                + ", edgeWeight=" + edgeWeight
                + ", messageType=" + messageType
                + '}';
    }
}
