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

package org.apache.geaflow.context.storage;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.context.api.model.Episode;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB-based persistent storage implementation for Phase 2.
 * Provides high-performance key-value storage for episodes, entities, and relations.
 */
public class RocksDBStore {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBStore.class);

    private final String dbPath;
    private final RocksDB episodesDb;
    private final RocksDB entitiesDb;
    private final RocksDB relationsDb;

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Constructor with storage path configuration.
     *
     * @param basePath Base directory path for RocksDB storage
     * @throws IOException if initialization fails
     */
    public RocksDBStore(String basePath) throws IOException {
        this.dbPath = basePath;
        try {
            // Create directory if not exists
            Path path = Paths.get(basePath);
            Files.createDirectories(path);

            // Initialize RocksDB instances
            Options options = new Options().setCreateIfMissing(true);
            
            this.episodesDb = RocksDB.open(
                options, Paths.get(basePath, "episodes").toString());
            this.entitiesDb = RocksDB.open(
                options, Paths.get(basePath, "entities").toString());
            this.relationsDb = RocksDB.open(
                options, Paths.get(basePath, "relations").toString());

            logger.info("RocksDB Store initialized at: {}", basePath);
        } catch (RocksDBException e) {
            logger.error("Error initializing RocksDB", e);
            throw new IOException("Failed to initialize RocksDB", e);
        }
    }

    /**
     * Add or update an episode.
     *
     * @param episode The episode to store
     * @throws IOException if operation fails
     */
    public void addEpisode(Episode episode) throws IOException {
        try {
            String json = JSON.toJSONString(episode);
            episodesDb.put(episode.getEpisodeId().getBytes(),
                json.getBytes());
            logger.debug("Episode stored: {}", episode.getEpisodeId());
        } catch (RocksDBException e) {
            throw new IOException("Error storing episode", e);
        }
    }

    /**
     * Get episode by ID.
     *
     * @param episodeId The episode ID
     * @return The episode or null if not found
     * @throws IOException if operation fails
     */
    public Episode getEpisode(String episodeId) throws IOException {
        try {
            byte[] data = episodesDb.get(episodeId.getBytes());
            if (data == null) {
                return null;
            }
            return JSON.parseObject(new String(data), Episode.class);
        } catch (RocksDBException e) {
            throw new IOException("Error retrieving episode", e);
        }
    }

    /**
     * Add or update an entity.
     *
     * @param entityId The entity ID
     * @param entity The entity to store
     * @throws IOException if operation fails
     */
    public void addEntity(String entityId, Episode.Entity entity) throws IOException {
        try {
            String json = JSON.toJSONString(entity);
            entitiesDb.put(entityId.getBytes(), json.getBytes());
        } catch (RocksDBException e) {
            throw new IOException("Error storing entity", e);
        }
    }

    /**
     * Get entity by ID.
     *
     * @param entityId The entity ID
     * @return The entity or null if not found
     * @throws IOException if operation fails
     */
    public Episode.Entity getEntity(String entityId) throws IOException {
        try {
            byte[] data = entitiesDb.get(entityId.getBytes());
            if (data == null) {
                return null;
            }
            return JSON.parseObject(new String(data), Episode.Entity.class);
        } catch (RocksDBException e) {
            throw new IOException("Error retrieving entity", e);
        }
    }

    /**
     * Add or update a relation.
     *
     * @param relationId The relation ID
     * @param relation The relation to store
     * @throws IOException if operation fails
     */
    public void addRelation(String relationId, Episode.Relation relation) throws IOException {
        try {
            String json = JSON.toJSONString(relation);
            relationsDb.put(relationId.getBytes(), json.getBytes());
        } catch (RocksDBException e) {
            throw new IOException("Error storing relation", e);
        }
    }

    /**
     * Get relation by ID.
     *
     * @param relationId The relation ID
     * @return The relation or null if not found
     * @throws IOException if operation fails
     */
    public Episode.Relation getRelation(String relationId) throws IOException {
        try {
            byte[] data = relationsDb.get(relationId.getBytes());
            if (data == null) {
                return null;
            }
            return JSON.parseObject(new String(data), Episode.Relation.class);
        } catch (RocksDBException e) {
            throw new IOException("Error retrieving relation", e);
        }
    }

    /**
     * Get count of stored items.
     *
     * @return Map with counts of episodes, entities, relations
     */
    public Map<String, Long> getStats() {
        Map<String, Long> stats = new HashMap<>();
        try {
            // Note: RocksDB doesn't provide direct count, 
            // this is a placeholder for estimation
            stats.put("episodes", -1L); // Would need iteration for actual count
            stats.put("entities", -1L);
            stats.put("relations", -1L);
        } catch (Exception e) {
            logger.warn("Error getting stats", e);
        }
        return stats;
    }

    /**
     * Close and cleanup resources.
     *
     * @throws IOException if close fails
     */
    public void close() throws IOException {
        try {
            if (episodesDb != null) {
                episodesDb.close();
            }
            if (entitiesDb != null) {
                entitiesDb.close();
            }
            if (relationsDb != null) {
                relationsDb.close();
            }
            logger.info("RocksDB Store closed");
        } catch (Exception e) {
            throw new IOException("Error closing RocksDB", e);
        }
    }
}
