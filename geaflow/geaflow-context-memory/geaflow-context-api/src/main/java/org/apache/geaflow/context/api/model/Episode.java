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

package org.apache.geaflow.context.api.model;

import com.alibaba.fastjson.JSON;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Episode is the core data unit for context memory.
 * It represents a contextual event with entities, relations, and temporal information.
 */
public class Episode implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Unique identifier for the episode.
     */
    private String episodeId;

    /**
     * Human-readable name of the episode.
     */
    private String name;

    /**
     * Time when the event occurred.
     */
    private long eventTime;

    /**
     * Time when the episode was ingested into the system.
     */
    private long ingestTime;

    /**
     * List of entities mentioned in this episode.
     */
    private List<Entity> entities;

    /**
     * List of relations between entities.
     */
    private List<Relation> relations;

    /**
     * Original content/text of the episode.
     */
    private String content;

    /**
     * Additional metadata.
     */
    private Map<String, Object> metadata;

    /**
     * Default constructor.
     */
    public Episode() {
        this.metadata = new HashMap<>();
        this.ingestTime = System.currentTimeMillis();
    }

    /**
     * Constructor with basic fields.
     */
    public Episode(String episodeId, String name, long eventTime, String content) {
        this();
        this.episodeId = episodeId;
        this.name = name;
        this.eventTime = eventTime;
        this.content = content;
    }

    // Getters and Setters
    public String getEpisodeId() {
        return episodeId;
    }

    public void setEpisodeId(String episodeId) {
        this.episodeId = episodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getIngestTime() {
        return ingestTime;
    }

    public void setIngestTime(long ingestTime) {
        this.ingestTime = ingestTime;
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    public List<Relation> getRelations() {
        return relations;
    }

    public void setRelations(List<Relation> relations) {
        this.relations = relations;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "Episode{"
                + "episodeId='" + episodeId + '\'
                + ", name='" + name + '\'
                + ", eventTime=" + eventTime
                + ", ingestTime=" + ingestTime
                + ", entities=" + (entities != null ? entities.size() : 0)
                + ", relations=" + (relations != null ? relations.size() : 0)
                + '}';
    }

    /**
     * Entity class representing a named entity in the context.
     */
    public static class Entity implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;
        private String name;
        private String type;
        private Map<String, Object> properties;

        public Entity() {
            this.properties = new HashMap<>();
        }

        public Entity(String id, String name, String type) {
            this();
            this.id = id;
            this.name = name;
            this.type = type;
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

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Override
        public String toString() {
            return "Entity{"
                    + "id='" + id + '\'
                    + ", name='" + name + '\'
                    + ", type='" + type + '\'
                    + '}';
        }
    }

    /**
     * Relation class representing a relationship between two entities.
     */
    public static class Relation implements Serializable {

        private static final long serialVersionUID = 1L;

        private String sourceId;
        private String targetId;
        private String relationshipType;
        private Map<String, Object> properties;

        public Relation() {
            this.properties = new HashMap<>();
        }

        public Relation(String sourceId, String targetId, String relationshipType) {
            this();
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.relationshipType = relationshipType;
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

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Override
        public String toString() {
            return "Relation{"
                    + "sourceId='" + sourceId + '\'
                    + ", targetId='" + targetId + '\'
                    + ", relationshipType='" + relationshipType + '\'
                    + '}';
        }
    }
}
