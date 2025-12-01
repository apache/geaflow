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

package org.apache.geaflow.context.api.result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ContextSearchResult represents the results of a context memory search.
 * Contains entities, relations, and relevance scores.
 */
public class ContextSearchResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /** List of entity results */
    private List<ContextEntity> entities;

    /** List of relation results */
    private List<ContextRelation> relations;

    /** Query execution time in milliseconds */
    private long executionTime;

    /** Total score/relevance metrics */
    private Map<String, Object> metrics;

    /**
     * Default constructor.
     */
    public ContextSearchResult() {
        this.entities = new ArrayList<>();
        this.relations = new ArrayList<>();
        this.metrics = new HashMap<>();
    }

    // Getters and Setters
    public List<ContextEntity> getEntities() {
        return entities;
    }

    public void setEntities(List<ContextEntity> entities) {
        this.entities = entities;
    }

    public List<ContextRelation> getRelations() {
        return relations;
    }

    public void setRelations(List<ContextRelation> relations) {
        this.relations = relations;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public void addEntity(ContextEntity entity) {
        this.entities.add(entity);
    }

    public void addRelation(ContextRelation relation) {
        this.relations.add(relation);
    }

    /**
     * ContextEntity represents an entity in the search result.
     */
    public static class ContextEntity implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;
        private String name;
        private String type;
        private double relevanceScore;
        private String source;
        private Map<String, Object> attributes;

        public ContextEntity() {
            this.attributes = new HashMap<>();
        }

        public ContextEntity(String id, String name, String type, double relevanceScore) {
            this();
            this.id = id;
            this.name = name;
            this.type = type;
            this.relevanceScore = relevanceScore;
        }

        // Getters and Setters
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public double getRelevanceScore() {
            return relevanceScore;
        }

        public void setRelevanceScore(double relevanceScore) {
            this.relevanceScore = relevanceScore;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "ContextEntity{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", relevanceScore=" + relevanceScore +
                    '}';
        }
    }

    /**
     * ContextRelation represents a relation in the search result.
     */
    public static class ContextRelation implements Serializable {

        private static final long serialVersionUID = 1L;

        private String sourceId;
        private String targetId;
        private String relationshipType;
        private double relevanceScore;
        private Map<String, Object> attributes;

        public ContextRelation() {
            this.attributes = new HashMap<>();
        }

        public ContextRelation(String sourceId, String targetId, String relationshipType,
                             double relevanceScore) {
            this();
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.relationshipType = relationshipType;
            this.relevanceScore = relevanceScore;
        }

        // Getters and Setters
        public String getSourceId() {
            return sourceId;
        }

        public void setSourceId(String sourceId) {
            this.sourceId = sourceId;
        }

        public String getTargetId() {
            return targetId;
        }

        public void setTargetId(String targetId) {
            this.targetId = targetId;
        }

        public String getRelationshipType() {
            return relationshipType;
        }

        public void setRelationshipType(String relationshipType) {
            this.relationshipType = relationshipType;
        }

        public double getRelevanceScore() {
            return relevanceScore;
        }

        public void setRelevanceScore(double relevanceScore) {
            this.relevanceScore = relevanceScore;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "ContextRelation{" +
                    "sourceId='" + sourceId + '\'' +
                    ", targetId='" + targetId + '\'' +
                    ", relationshipType='" + relationshipType + '\'' +
                    ", relevanceScore=" + relevanceScore +
                    '}';
        }
    }
}
