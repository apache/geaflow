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

package org.apache.geaflow.context.core.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Graph traversal search implementation for Phase 2.
 * Performs BFS (Breadth-First Search) to find related entities.
 */
public class GraphTraversalSearch {

    private static final Logger logger = LoggerFactory.getLogger(
        GraphTraversalSearch.class);

    private final Map<String, Episode.Entity> entities;
    private final Map<String, List<Episode.Relation>> entityRelations;

    /**
     * Constructor with entity and relation maps.
     *
     * @param entities Map of entities by ID
     * @param relations List of all relations
     */
    public GraphTraversalSearch(Map<String, Episode.Entity> entities,
        List<Episode.Relation> relations) {
        this.entities = entities;
        this.entityRelations = buildEntityRelationMap(relations);
    }

    /**
     * Search for entities related to a query entity through graph traversal.
     *
     * @param seedEntityId Starting entity ID
     * @param maxHops Maximum traversal hops
     * @param maxResults Maximum results to return
     * @return Search result with related entities
     */
    public ContextSearchResult search(String seedEntityId, int maxHops,
        int maxResults) {
        ContextSearchResult result = new ContextSearchResult();

        if (!entities.containsKey(seedEntityId)) {
            logger.warn("Seed entity not found: {}", seedEntityId);
            return result;
        }

        Set<String> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        Map<String, Integer> distances = new HashMap<>();

        // BFS traversal
        queue.offer(seedEntityId);
        visited.add(seedEntityId);
        distances.put(seedEntityId, 0);

        while (!queue.isEmpty() && result.getEntities().size() < maxResults) {
            String currentId = queue.poll();
            int currentDistance = distances.get(currentId);

            // Add current entity to results
            Episode.Entity entity = entities.get(currentId);
            if (entity != null && !currentId.equals(seedEntityId)) {
                ContextSearchResult.ContextEntity contextEntity =
                    new ContextSearchResult.ContextEntity(
                        entity.getId(),
                        entity.getName(),
                        entity.getType(),
                        1.0 / (1.0 + currentDistance) // Distance-based relevance
                    );
                result.addEntity(contextEntity);
            }

            // Explore neighbors if within hop limit
            if (currentDistance < maxHops) {
                List<Episode.Relation> relations = 
                    entityRelations.getOrDefault(currentId, new ArrayList<>());
                for (Episode.Relation relation : relations) {
                    String nextId = relation.getTargetId();
                    if (!visited.contains(nextId)) {
                        visited.add(nextId);
                        distances.put(nextId, currentDistance + 1);
                        queue.offer(nextId);
                    }
                }
            }
        }

        logger.debug("Graph traversal found {} entities within {} hops",
            result.getEntities().size(), maxHops);

        return result;
    }

    /**
     * Build a map of entity ID to outgoing relations.
     *
     * @param relations List of all relations
     * @return Map of entity ID to relations
     */
    private Map<String, List<Episode.Relation>> buildEntityRelationMap(
        List<Episode.Relation> relations) {
        Map<String, List<Episode.Relation>> relationMap = new HashMap<>();
        if (relations != null) {
            for (Episode.Relation relation : relations) {
                relationMap.computeIfAbsent(relation.getSourceId(),
                    k -> new ArrayList<>()).add(relation);
            }
        }
        return relationMap;
    }
}
