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
 * MST edge class.
 * Represents an edge in the minimum spanning tree, containing source vertex, target vertex and weight information.
 * 
 * <p>Supported operations:
 * - Create edge
 * - Get edge endpoints
 * - Check if it is a self-loop
 * - Create reverse edge
 * - Compare edges (by weight and endpoints)
 * 
 * @author Geaflow Team
 */
public class MSTEdge implements Serializable, Comparable<MSTEdge> {
    
    private static final long serialVersionUID = 1L;
    
    /** Source vertex ID. */
    private Object sourceId;
    
    /** Target vertex ID. */
    private Object targetId;
    
    /** Edge weight. */
    private double weight;

    /**
     * Constructor.
     * @param sourceId Source vertex ID
     * @param targetId Target vertex ID
     * @param weight Edge weight
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
     * Get the other endpoint of the edge.
     * @param vertexId Known vertex ID
     * @return Other endpoint ID, returns null if vertexId is not an endpoint of the edge
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
     * Check if specified vertex is an endpoint of the edge.
     * @param vertexId Vertex ID
     * @return Whether it is an endpoint
     */
    public boolean isEndpoint(Object vertexId) {
        return sourceId.equals(vertexId) || targetId.equals(vertexId);
    }

    /**
     * Check if it is a self-loop edge.
     * @return Whether it is a self-loop
     */
    public boolean isSelfLoop() {
        return sourceId.equals(targetId);
    }

    /**
     * Create reverse edge.
     * @return Reverse edge
     */
    public MSTEdge reverse() {
        return new MSTEdge(targetId, sourceId, weight);
    }

    /**
     * Check if two edges are equal (ignoring direction).
     * @param other Another edge
     * @return Whether they are equal
     */
    public boolean equalsIgnoreDirection(MSTEdge other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        // Quick weight comparison first (fastest check)
        if (Double.compare(other.weight, weight) != 0) {
            return false;
        }

        // Short-circuit direction comparison using OR operator
        return (Objects.equals(sourceId, other.sourceId) && Objects.equals(targetId, other.targetId))
            || (Objects.equals(sourceId, other.targetId) && Objects.equals(targetId, other.sourceId));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MSTEdge edge = (MSTEdge) obj;
        return Double.compare(edge.weight, weight) == 0
            && Objects.equals(sourceId, edge.sourceId)
            && Objects.equals(targetId, edge.targetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, targetId, weight);
    }

    @Override
    public int compareTo(MSTEdge other) {
        // First compare by weight
        int weightCompare = Double.compare(this.weight, other.weight);
        if (weightCompare != 0) {
            return weightCompare;
        }
        
        // If weights are equal, compare by source vertex ID
        int sourceCompare = sourceId.toString().compareTo(other.sourceId.toString());
        if (sourceCompare != 0) {
            return sourceCompare;
        }
        
        // If source vertex IDs are equal, compare by target vertex ID
        return targetId.toString().compareTo(other.targetId.toString());
    }

    @Override
    public String toString() {
        return "MSTEdge{"
                + "sourceId=" + sourceId
                + ", targetId=" + targetId
                + ", weight=" + weight
                + '}';
    }
} 