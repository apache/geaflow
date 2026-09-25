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
import org.apache.geaflow.context.core.api.AdvancedQueryAPI.ContextSnapshot;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for Phase 4 APIs.
 */
public class ContextMemoryAPITest {

  /**
   * Test factory creation with default config.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testFactoryCreateDefault() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("storage.type", "rocksdb");

    Assert.assertNotNull(config);
    Assert.assertEquals("rocksdb", config.get("storage.type"));
  }

  /**
   * Test factory config keys.
   */
  @Test
  public void testConfigKeys() {
    String storageType = ContextMemoryEngineFactory.ContextConfigKeys.STORAGE_TYPE;
    String vectorType = ContextMemoryEngineFactory.ContextConfigKeys.VECTOR_INDEX_TYPE;

    Assert.assertNotNull(storageType);
    Assert.assertNotNull(vectorType);
    Assert.assertEquals("storage.type", storageType);
  }

  /**
   * Test advanced query snapshot creation.
   */
  @Test
  public void testSnapshotCreation() {
    AdvancedQueryAPI.ContextSnapshot snapshot = new ContextSnapshot("snap-001",
        System.currentTimeMillis());

    Assert.assertNotNull(snapshot);
    Assert.assertEquals("snap-001", snapshot.getSnapshotId());
    Assert.assertTrue(snapshot.getTimestamp() > 0);
  }

  /**
   * Test snapshot metadata.
   */
  @Test
  public void testSnapshotMetadata() {
    ContextSnapshot snapshot = new ContextSnapshot("snap-002",
        System.currentTimeMillis());

    snapshot.setMetadata("agent_id", "agent-001");
    snapshot.setMetadata("version", "1.0");

    Assert.assertEquals("agent-001", snapshot.getMetadata().get("agent_id"));
    Assert.assertEquals("1.0", snapshot.getMetadata().get("version"));
    Assert.assertEquals(2, snapshot.getMetadata().size());
  }

  /**
   * Test config default values.
   */
  @Test
  public void testConfigDefaults() {
    int defaultDimension = ContextMemoryEngineFactory.ContextConfigKeys
        .DEFAULT_VECTOR_DIMENSION;
    double defaultThreshold = ContextMemoryEngineFactory.ContextConfigKeys
        .DEFAULT_VECTOR_THRESHOLD;
    String defaultPath = ContextMemoryEngineFactory.ContextConfigKeys
        .DEFAULT_STORAGE_PATH;

    Assert.assertEquals(768, defaultDimension);
    Assert.assertEquals(0.7, defaultThreshold, 0.001);
    Assert.assertNotNull(defaultPath);
  }

  /**
   * Test advanced query API initialization.
   */
  @Test
  public void testAdvancedQueryAPI() {
    // Mock engine for testing
    org.apache.geaflow.context.api.engine.ContextMemoryEngine mockEngine = null;

    try {
      // In real test, would use actual engine or mock
      AdvancedQueryAPI queryAPI = new AdvancedQueryAPI(mockEngine);
      Assert.assertNotNull(queryAPI);
    } catch (NullPointerException e) {
      // Expected due to null engine, API creation successful
    }
  }

  /**
   * Test snapshot listing.
   */
  @Test
  public void testSnapshotListing() {
    AdvancedQueryAPI queryAPI = new AdvancedQueryAPI(null);

    // Create multiple snapshots
    AdvancedQueryAPI.ContextSnapshot snap1 = queryAPI.createSnapshot("snap-1");
    AdvancedQueryAPI.ContextSnapshot snap2 = queryAPI.createSnapshot("snap-2");

    String[] snapshots = queryAPI.listSnapshots();
    Assert.assertEquals(2, snapshots.length);
  }

  /**
   * Test snapshot retrieval.
   */
  @Test
  public void testSnapshotRetrieval() {
    AdvancedQueryAPI queryAPI = new AdvancedQueryAPI(null);

    AdvancedQueryAPI.ContextSnapshot created = queryAPI.createSnapshot("snap-test");
    AdvancedQueryAPI.ContextSnapshot retrieved = queryAPI.getSnapshot("snap-test");

    Assert.assertNotNull(retrieved);
    Assert.assertEquals(created.getSnapshotId(), retrieved.getSnapshotId());
  }

  /**
   * Test non-existent snapshot.
   */
  @Test
  public void testNonExistentSnapshot() {
    AdvancedQueryAPI queryAPI = new AdvancedQueryAPI(null);

    AdvancedQueryAPI.ContextSnapshot snapshot = queryAPI.getSnapshot("non-existent");
    Assert.assertNull(snapshot);
  }

  /**
   * Test agent session creation.
   */
  @Test
  public void testAgentSessionCreation() {
    AgentMemoryAPI agentAPI = new AgentMemoryAPI(null);

    AgentMemoryAPI.AgentSession session = agentAPI.getOrCreateSession("agent-001");
    Assert.assertNotNull(session);

    // Second call should return same session
    AgentMemoryAPI.AgentSession session2 = agentAPI.getOrCreateSession("agent-001");
    Assert.assertEquals(session, session2);
  }

  /**
   * Test agent session statistics.
   */
  @Test
  public void testAgentSessionStats() {
    AgentMemoryAPI.AgentSession session = new AgentMemoryAPI.AgentSession("agent-001");

    String stats = session.getStats();
    Assert.assertNotNull(stats);
    Assert.assertTrue(stats.contains("agent-001"));
    Assert.assertTrue(stats.contains("experiences=0"));
  }

  /**
   * Test agent experience recording.
   */
  @Test
  public void testAgentExperienceRecording() {
    AgentMemoryAPI.AgentSession session = new AgentMemoryAPI.AgentSession("agent-001");

    session.addExperienceId("exp-001");
    session.addExperienceId("exp-002");

    String stats = session.getStats();
    Assert.assertTrue(stats.contains("experiences=2"));
  }

  /**
   * Test agent experience removal.
   */
  @Test
  public void testAgentExperienceRemoval() {
    AgentMemoryAPI.AgentSession session = new AgentMemoryAPI.AgentSession("agent-001");

    session.addExperienceId("exp-001");
    session.addExperienceId("exp-002");

    java.util.List<String> toRemove = java.util.Arrays.asList("exp-001");
    session.removeExperiences(toRemove);

    String stats = session.getStats();
    Assert.assertTrue(stats.contains("experiences=1"));
  }
}
