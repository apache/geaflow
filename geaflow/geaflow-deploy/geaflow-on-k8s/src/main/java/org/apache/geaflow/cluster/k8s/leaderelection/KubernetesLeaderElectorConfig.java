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

package org.apache.geaflow.cluster.k8s.leaderelection;

import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.common.config.Configuration;

public class KubernetesLeaderElectorConfig {

    /**
     * Cluster namespace.
     */
    private String namespace;

    /**
     * Identity of the contender.
     */
    private String identity;

    /**
     * The name of the config-map-lock.
     */
    private String configMapName;

    /**
     * Duration of the lease.
     */
    private Integer leaseDuration;

    /**
     * The deadline of the leader to renew the lease.
     */
    private Integer renewDeadline;

    /**
     * Retry interval of the contender.
     */
    private Integer retryPeriod;

    public static KubernetesLeaderElectorConfig build(Configuration configuration,
                                                      String configMapName, String identity) {
        KubernetesLeaderElectorConfig electorConfig = new KubernetesLeaderElectorConfig();
        electorConfig.setNamespace(configuration.getString(KubernetesConfigKeys.NAME_SPACE));
        electorConfig.setLeaseDuration(configuration.getInteger(KubernetesConfigKeys.LEADER_ELECTION_LEASE_DURATION));
        electorConfig.setRenewDeadline(configuration.getInteger(KubernetesConfigKeys.LEADER_ELECTION_RENEW_DEADLINE));
        electorConfig.setRetryPeriod(configuration.getInteger(KubernetesConfigKeys.LEADER_ELECTION_RETRY_PERIOD));
        electorConfig.setIdentity(identity);
        electorConfig.setConfigMapName(configMapName);
        return electorConfig;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getConfigMapName() {
        return configMapName;
    }

    public void setConfigMapName(String configMapName) {
        this.configMapName = configMapName;
    }

    public Integer getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Integer leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public Integer getRenewDeadline() {
        return renewDeadline;
    }

    public void setRenewDeadline(Integer renewDeadline) {
        this.renewDeadline = renewDeadline;
    }

    public Integer getRetryPeriod() {
        return retryPeriod;
    }

    public void setRetryPeriod(Integer retryPeriod) {
        this.retryPeriod = retryPeriod;
    }
}
