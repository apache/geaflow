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

package org.apache.geaflow.context.core.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.query.ContextQuery.RetrievalStrategy;
import org.apache.geaflow.context.api.query.ContextQuery.TemporalFilter;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.apache.geaflow.context.api.model.Episode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent Memory API for high-level agent-specific memory management.
 * Supports agent sessions, memory states, and contextual recall.
 */
public class AgentMemoryAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(AgentMemoryAPI.class);

  private final ContextMemoryEngine engine;
  private final Map<String, AgentSession> sessions;

  /**
   * Constructor with engine.
   *
   * @param engine The ContextMemoryEngine to use
   */
  public AgentMemoryAPI(ContextMemoryEngine engine) {
    this.engine = engine;
    this.sessions = new HashMap<>();
  }

  /**
   * Create or retrieve an agent session.
   *

   * @param agentId The agent ID
   * @return The agent session
   */
  public AgentSession getOrCreateSession(String agentId) {
    return sessions.computeIfAbsent(agentId, k -> new AgentSession(agentId));
  }

  /**
   * Store an experience in the agent's memory.
   *

   * @param agentId The agent ID
   * @param experience The experience description
   * @return The stored episode ID
   * @throws Exception if storage fails
   */
  public String recordExperience(String agentId, String experience) throws Exception {
    AgentSession session = getOrCreateSession(agentId);

    Episode episode = new Episode();
    episode.setEpisodeId("agent-" + agentId + "-" + System.currentTimeMillis());
    episode.setName(agentId + "_experience");
    episode.setContent(experience);
    episode.setEventTime(System.currentTimeMillis());
    episode.setIngestTime(System.currentTimeMillis());

    engine.ingestEpisode(episode);
    session.addExperienceId(episode.getEpisodeId());

    LOGGER.info("Recorded experience for agent {}: {}", agentId, episode.getEpisodeId());
    return episode.getEpisodeId();
  }

  /**
   * Recall relevant context for an agent.
   *

   * @param agentId The agent ID
   * @param query The query text
   * @param maxResults Maximum results to return
   * @return The search results
   * @throws Exception if search fails
   */
  public ContextSearchResult recall(String agentId, String query, int maxResults)
      throws Exception {
    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(query)
        .strategy(RetrievalStrategy.HYBRID)
        .maxHops(2)
        .vectorThreshold(0.7)
        .build();

    ContextSearchResult result = engine.search(contextQuery);
    LOGGER.info("Recall for agent {}: {} entities found", agentId, result.getEntities().size());
    return result;
  }

  /**
   * Clear old experiences from memory.
   *

   * @param agentId The agent ID
   * @param retentionDays Number of days to retain
   * @throws Exception if cleanup fails
   */
  public void clearOldExperiences(String agentId, int retentionDays) throws Exception {
    AgentSession session = getOrCreateSession(agentId);
    long cutoffTime = System.currentTimeMillis() - (retentionDays * 24L * 3600000L);

    List<String> experiencesToRemove = new ArrayList<>();
    // In production, would query and remove old episodes
    session.removeExperiences(experiencesToRemove);

    LOGGER.info("Cleared {} old experiences for agent {}", experiencesToRemove.size(), agentId);
  }

  /**
   * Get agent session statistics.
   *

   * @param agentId The agent ID
   * @return Session statistics as a string
   */
  public String getSessionStats(String agentId) {
    AgentSession session = getOrCreateSession(agentId);
    return session.getStats();
  }

  /**
   * Agent session state holder.
   */
  public static class AgentSession {

    private final String agentId;
    private final List<String> experienceIds;
    private long createdTime;
    private long lastAccessTime;

    public AgentSession(String agentId) {
      this.agentId = agentId;
      this.experienceIds = new ArrayList<>();
      this.createdTime = System.currentTimeMillis();
      this.lastAccessTime = System.currentTimeMillis();
    }

    public void addExperienceId(String experienceId) {
      experienceIds.add(experienceId);
      lastAccessTime = System.currentTimeMillis();
    }

    public void removeExperiences(List<String> idsToRemove) {
      experienceIds.removeAll(idsToRemove);
      lastAccessTime = System.currentTimeMillis();
    }

    public String getStats() {
      long duration = System.currentTimeMillis() - createdTime;
      return String.format(
          "AgentSession{id='%s', experiences=%d, duration=%dms}",
          agentId, experienceIds.size(), duration);
    }
  }
}
