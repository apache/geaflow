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

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.query.ContextQuery.RetrievalStrategy;
import org.apache.geaflow.context.api.query.ContextQuery.TemporalFilter;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advanced Query API supporting temporal queries and snapshots.
 */
public class AdvancedQueryAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedQueryAPI.class);

  private final ContextMemoryEngine engine;
  private final Map<String, ContextSnapshot> snapshots;

  /**
   * Constructor.
   *
   * @param engine The ContextMemoryEngine
   */
  public AdvancedQueryAPI(ContextMemoryEngine engine) {
    this.engine = engine;
    this.snapshots = new HashMap<>();
  }

  /**
   * Query with time range filter.
   *

   * @param query The query text
   * @param startTime Start time in milliseconds
   * @param endTime End time in milliseconds
   * @return Search results
   * @throws Exception if query fails
   */
  public ContextSearchResult queryTimeRange(String query, long startTime, long endTime)
      throws Exception {
    TemporalFilter timeFilter = new TemporalFilter();
    timeFilter.setStartTime(startTime);
    timeFilter.setEndTime(endTime);

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(query)
        .strategy(RetrievalStrategy.HYBRID)
        .timeRange(timeFilter)
        .maxHops(2)
        .build();

    ContextSearchResult result = engine.search(contextQuery);
    LOGGER.info("Temporal query returned {} entities", result.getEntities().size());
    return result;
  }

  /**
   * Query for entities as of a specific time.
   *

   * @param query The query text
   * @param timestamp The timestamp to query at
   * @return Search results
   * @throws Exception if query fails
   */
  public ContextSearchResult queryAtTime(String query, long timestamp) throws Exception {
    return queryTimeRange(query, 0, timestamp);
  }

  /**
   * Create a snapshot of current context state.
   *

   * @param snapshotId The snapshot ID
   * @return The snapshot
   */
  public ContextSnapshot createSnapshot(String snapshotId) {
    ContextSnapshot snapshot = new ContextSnapshot(snapshotId, System.currentTimeMillis());
    snapshots.put(snapshotId, snapshot);
    LOGGER.info("Created snapshot: {}", snapshotId);
    return snapshot;
  }

  /**
   * Retrieve a snapshot.
   *

   * @param snapshotId The snapshot ID
   * @return The snapshot, or null if not found
   */
  public ContextSnapshot getSnapshot(String snapshotId) {
    return snapshots.get(snapshotId);
  }

  /**
   * List all snapshots.
   *

   * @return Array of snapshot IDs
   */
  public String[] listSnapshots() {
    return snapshots.keySet().toArray(new String[0]);
  }

  /**
   * Context snapshot for versioning.
   */
  public static class ContextSnapshot {

    private final String snapshotId;
    private final long timestamp;
    private final Map<String, Object> metadata;

    public ContextSnapshot(String snapshotId, long timestamp) {
      this.snapshotId = snapshotId;
      this.timestamp = timestamp;
      this.metadata = new HashMap<>();
    }

    public String getSnapshotId() {
      return snapshotId;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Map<String, Object> getMetadata() {
      return metadata;
    }

    public void setMetadata(String key, Object value) {
      metadata.put(key, value);
    }

    @Override
    public String toString() {
      return "ContextSnapshot{"
          + "id='" + snapshotId + '\''
          + ", timestamp=" + timestamp
          + ", metadata=" + metadata.size() + " entries"
          + '}';
    }
  }
}
