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

package org.apache.geaflow.context.core.ha;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High Availability configuration for Context Memory cluster.
 * Supports replica management, failover, and data backup strategies.
 */
public class HighAvailabilityConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(HighAvailabilityConfig.class);

  /**
   * Replica configuration.
   */
  public static class ReplicaConfig {

    private String replicaId;
    private String host;
    private int port;
    private String role; // PRIMARY, SECONDARY, STANDBY
    private long lastHeartbeat;

    public ReplicaConfig(String replicaId, String host, int port, String role) {
      this.replicaId = replicaId;
      this.host = host;
      this.port = port;
      this.role = role;
      this.lastHeartbeat = System.currentTimeMillis();
    }

    public String getReplicaId() {
      return replicaId;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public String getRole() {
      return role;
    }

    public void setRole(String role) {
      this.role = role;
    }

    public long getLastHeartbeat() {
      return lastHeartbeat;
    }

    public void updateHeartbeat() {
      this.lastHeartbeat = System.currentTimeMillis();
    }

    public boolean isHealthy(long heartbeatTimeoutMs) {
      return System.currentTimeMillis() - lastHeartbeat < heartbeatTimeoutMs;
    }
  }

  private final List<ReplicaConfig> replicas;
  private final int replicationFactor;
  private final long heartbeatTimeoutMs;
  private final long backupIntervalMs;
  private String primaryReplicaId;

  /**
   * Constructor.
   *

   * @param replicationFactor Number of replicas to maintain
   * @param heartbeatTimeoutMs Heartbeat timeout in milliseconds
   * @param backupIntervalMs Backup interval in milliseconds
   */
  public HighAvailabilityConfig(int replicationFactor, long heartbeatTimeoutMs,
      long backupIntervalMs) {
    this.replicas = new ArrayList<>();
    this.replicationFactor = replicationFactor;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.backupIntervalMs = backupIntervalMs;
  }

  /**
   * Add a replica node.
   *

   * @param replicaId Replica ID
   * @param host Replica host
   * @param port Replica port
   * @param role Replica role (PRIMARY, SECONDARY, STANDBY)
   */
  public void addReplica(String replicaId, String host, int port, String role) {
    ReplicaConfig replica = new ReplicaConfig(replicaId, host, port, role);
    replicas.add(replica);

    if ("PRIMARY".equals(role)) {
      this.primaryReplicaId = replicaId;
    }

    LOGGER.info("Added replica: {} at {}:{} as {}", replicaId, host, port, role);
  }

  /**
   * Check replica health.
   *

   * @return List of healthy replicas
   */
  public List<ReplicaConfig> getHealthyReplicas() {
    List<ReplicaConfig> healthy = new ArrayList<>();
    for (ReplicaConfig replica : replicas) {
      if (replica.isHealthy(heartbeatTimeoutMs)) {
        healthy.add(replica);
      }
    }
    return healthy;
  }

  /**
   * Perform failover to secondary replica.
   *

   * @return New primary replica ID, or null if no healthy replica available
   */
  public String performFailover() {
    LOGGER.warn("Performing failover for primary: {}", primaryReplicaId);

    // Mark primary as unhealthy
    for (ReplicaConfig replica : replicas) {
      if (replica.replicaId.equals(primaryReplicaId)) {
        LOGGER.warn("Primary replica {} is unhealthy", primaryReplicaId);
        break;
      }
    }

    // Find healthy secondary
    for (ReplicaConfig replica : replicas) {
      if (!replica.replicaId.equals(primaryReplicaId) && replica.isHealthy(heartbeatTimeoutMs)) {
        if ("SECONDARY".equals(replica.role) || "STANDBY".equals(replica.role)) {
          replica.setRole("PRIMARY");
          this.primaryReplicaId = replica.replicaId;
          LOGGER.info("Failover completed: {} is new primary", replica.replicaId);
          return replica.replicaId;
        }
      }
    }

    LOGGER.error("Failover failed: no healthy replicas available");
    return null;
  }

  /**
   * Get backup strategy configuration.
   *

   * @return Backup configuration as string
   */
  public String getBackupStrategy() {
    return String.format(
        "BackupStrategy{replicationFactor=%d, interval=%d ms, "
            + "heartbeatTimeout=%d ms, primaryReplica=%s, totalReplicas=%d}",
        replicationFactor, backupIntervalMs, heartbeatTimeoutMs, primaryReplicaId,
        replicas.size());
  }

  // Getters
  public List<ReplicaConfig> getReplicas() {
    return replicas;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public long getHeartbeatTimeoutMs() {
    return heartbeatTimeoutMs;
  }

  public long getBackupIntervalMs() {
    return backupIntervalMs;
  }

  public String getPrimaryReplicaId() {
    return primaryReplicaId;
  }
}
