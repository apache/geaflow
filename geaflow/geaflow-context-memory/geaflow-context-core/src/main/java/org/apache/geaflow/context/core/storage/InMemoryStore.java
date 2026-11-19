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

package org.apache.geaflow.context.core.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.context.api.model.Episode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory storage implementation for Phase 1.
 * Not recommended for production; use persistent storage backends for production.
 */
public class InMemoryStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

    private final Map<String, Episode> episodes;
    private final Map<String, Episode.Entity> entities;
    private final Map<String, Episode.Relation> relations;

    public InMemoryStore() {
        this.episodes = new ConcurrentHashMap<>();
        this.entities = new ConcurrentHashMap<>();
        this.relations = new ConcurrentHashMap<>();
    }

    /**
     * Initialize the store.
     *
     * @throws Exception if initialization fails
     */
    public void initialize() throws Exception {
        logger.info("Initializing InMemoryStore");
    }

    /**
     * Add or update an episode.
     *
     * @param episode The episode to add
     */
    public void addEpisode(Episode episode) {
        episodes.put(episode.getEpisodeId(), episode);
        logger.debug("Added episode: {}", episode.getEpisodeId());
    }

    /**
     * Get episode by ID.
     *
     * @param episodeId The episode ID
     * @return The episode, or null if not found
     */
    public Episode getEpisode(String episodeId) {
        return episodes.get(episodeId);
    }

    /**
     * Add or update an entity.
     *
     * @param entityId The entity ID
     * @param entity The entity
     */
    public void addEntity(String entityId, Episode.Entity entity) {
        entities.put(entityId, entity);
    }

    /**
     * Get entity by ID.
     *
     * @param entityId The entity ID
     * @return The entity, or null if not found
     */
    public Episode.Entity getEntity(String entityId) {
        return entities.get(entityId);
    }

    /**
     * Get all entities.
     *
     * @return Map of all entities
     */
    public Map<String, Episode.Entity> getEntities() {
        return new HashMap<>(entities);
    }

    /**
     * Add or update a relation.
     *
     * @param relationId The relation ID (usually "source->target")
     * @param relation The relation
     */
    public void addRelation(String relationId, Episode.Relation relation) {
        relations.put(relationId, relation);
    }

    /**
     * Get relation by ID.
     *
     * @param relationId The relation ID
     * @return The relation, or null if not found
     */
    public Episode.Relation getRelation(String relationId) {
        return relations.get(relationId);
    }

    /**
     * Get all relations.
     *
     * @return Map of all relations
     */
    public Map<String, Episode.Relation> getRelations() {
        return new HashMap<>(relations);
    }

    /**
     * Get statistics about the store.
     *
     * @return Store statistics
     */
    public StoreStats getStats() {
        return new StoreStats(episodes.size(), entities.size(), relations.size());
    }

    /**
     * Clear all data.
     */
    public void clear() {
        episodes.clear();
        entities.clear();
        relations.clear();
        logger.info("InMemoryStore cleared");
    }

    /**
     * Close and cleanup.
     *
     * @throws Exception if close fails
     */
    public void close() throws Exception {
        clear();
        logger.info("InMemoryStore closed");
    }

    /**
     * Store statistics.
     */
    public static class StoreStats {

        private final int episodeCount;
        private final int entityCount;
        private final int relationCount;

        public StoreStats(int episodeCount, int entityCount, int relationCount) {
            this.episodeCount = episodeCount;
            this.entityCount = entityCount;
            this.relationCount = relationCount;
        }

        public int getEpisodeCount() {
            return episodeCount;
        }

        public int getEntityCount() {
            return entityCount;
        }

        public int getRelationCount() {
            return relationCount;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("StoreStats{")
                    .append("episodes=")
                    .append(episodeCount)
                    .append(", entities=")
                    .append(entityCount)
                    .append(", relations=")
                    .append(relationCount)
                    .append("}")
                    .toString();
        }
    }
}
