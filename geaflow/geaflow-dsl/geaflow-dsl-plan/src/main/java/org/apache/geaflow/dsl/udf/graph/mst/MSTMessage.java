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
import java.util.Objects;

/**
 * MST message class.
 * Used for message passing between vertices, supporting different types of MST operations.
 * 
 * <p>Message types:
 * - COMPONENT_UPDATE: Component update message
 * - EDGE_PROPOSAL: Edge proposal message
 * - EDGE_ACCEPTANCE: Edge acceptance message
 * - EDGE_REJECTION: Edge rejection message
 * - MST_EDGE_FOUND: MST edge discovery message
 * 
 * @author TuGraph Analytics Team
 */
public class MSTMessage implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** Message type enumeration. */
    public enum MessageType {
        /** Component update message. */
        COMPONENT_UPDATE,
        /** Edge proposal message. */
        EDGE_PROPOSAL,
        /** Edge acceptance message. */
        EDGE_ACCEPTANCE,
        /** Edge rejection message. */
        EDGE_REJECTION,
        /** MST edge discovery message. */
        MST_EDGE_FOUND
    }

    /** Message type. */
    private MessageType type;
    
    /** Source vertex ID. */
    private Object sourceId;
    
    /** Target vertex ID. */
    private Object targetId;
    
    /** Edge weight. */
    private double weight;
    
    /** Component ID. */
    private Object componentId;
    
    /** MST edge. */
    private MSTEdge edge;
    
    /** Message timestamp. */
    private long timestamp;

    /**
     * Constructor.
     * @param type Message type
     * @param sourceId Source vertex ID
     * @param targetId Target vertex ID
     * @param weight Edge weight
     */
    public MSTMessage(MessageType type, Object sourceId, Object targetId, double weight) {
        this.type = type;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.weight = weight;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Constructor with component ID.
     * @param type Message type
     * @param sourceId Source vertex ID
     * @param targetId Target vertex ID
     * @param weight Edge weight
     * @param componentId Component ID
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
     * Check if this is a component update message.
     * @return Whether this is a component update message
     */
    public boolean isComponentUpdate() {
        return type == MessageType.COMPONENT_UPDATE;
    }

    /**
     * Check if this is an edge proposal message.
     * @return Whether this is an edge proposal message
     */
    public boolean isEdgeProposal() {
        return type == MessageType.EDGE_PROPOSAL;
    }

    /**
     * Check if this is an edge acceptance message.
     * @return Whether this is an edge acceptance message
     */
    public boolean isEdgeAcceptance() {
        return type == MessageType.EDGE_ACCEPTANCE;
    }

    /**
     * Check if this is an edge rejection message.
     * @return Whether this is an edge rejection message
     */
    public boolean isEdgeRejection() {
        return type == MessageType.EDGE_REJECTION;
    }

    /**
     * Check if this is an MST edge discovery message.
     * @return Whether this is an MST edge discovery message
     */
    public boolean isMSTEdgeFound() {
        return type == MessageType.MST_EDGE_FOUND;
    }

    /**
     * Check if the message is expired.
     * @param currentTime Current time
     * @param timeout Timeout duration (milliseconds)
     * @return Whether the message is expired
     */
    public boolean isExpired(long currentTime, long timeout) {
        return (currentTime - timestamp) > timeout;
    }

    /**
     * Create a copy of the message.
     * @return Message copy
     */
    public MSTMessage copy() {
        MSTMessage copy = new MSTMessage(type, sourceId, targetId, weight, componentId);
        copy.setEdge(edge);
        copy.setTimestamp(timestamp);
        return copy;
    }

    /**
     * Create a reverse message.
     * @return Reverse message
     */
    public MSTMessage reverse() {
        MSTMessage reverse = new MSTMessage(type, targetId, sourceId, weight, componentId);
        reverse.setEdge(edge);
        reverse.setTimestamp(timestamp);
        return reverse;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MSTMessage message = (MSTMessage) obj;
        return Double.compare(message.weight, weight) == 0
            && timestamp == message.timestamp
            && type == message.type
            && Objects.equals(sourceId, message.sourceId)
            && Objects.equals(targetId, message.targetId)
            && Objects.equals(componentId, message.componentId)
            && Objects.equals(edge, message.edge);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, sourceId, targetId, weight, componentId, edge, timestamp);
    }

    @Override
    public String toString() {
        return "MSTMessage{"
                + "type=" + type
                + ", sourceId=" + sourceId
                + ", targetId=" + targetId
                + ", weight=" + weight
                + ", componentId=" + componentId
                + ", edge=" + edge
                + ", timestamp=" + timestamp
                + '}';
    }
} 