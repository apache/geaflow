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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.ConfigMapLock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.common.utils.ThreadUtil;

public class KubernetesLeaderElector {

    public static final String LEADER_ANNOTATION_KEY = "control-plane.alpha.kubernetes.io/leader";

    private final ExecutorService executorService = Executors.newFixedThreadPool(4,
        ThreadUtil.namedThreadFactory(true, "leader-elector"));

    private final LeaderElector innerLeaderElector;

    public KubernetesLeaderElector(GeaflowKubeClient client, KubernetesLeaderElectorConfig config,
                                   LeaderCallbacks callbacks) {
        ConfigMapLock lock = new ConfigMapLock(config.getNamespace(), config.getConfigMapName(), config.getIdentity());
        LeaderElectionConfig electionConfig = new LeaderElectionConfigBuilder()
            .withName(config.getConfigMapName())
            .withLock(lock)
            .withLeaseDuration(Duration.ofSeconds(config.getLeaseDuration()))
            .withRenewDeadline(Duration.ofSeconds(config.getRenewDeadline()))
            .withRetryPeriod(Duration.ofSeconds(config.getRetryPeriod()))
            .withLeaderCallbacks(callbacks)
            .build();

        innerLeaderElector = client.createLeaderElector(electionConfig, executorService);
    }

    public void run() {
        if (!executorService.isShutdown()) {
            executorService.execute(innerLeaderElector::run);
        }
    }

    public void close() {
        executorService.shutdownNow();
    }

    public static boolean isLeader(ConfigMap configMap, String identity) {
        Map<String, String> annotations =
            Optional.ofNullable(configMap.getMetadata().getAnnotations()).orElse(new HashMap<>());
        String leader = annotations.get(LEADER_ANNOTATION_KEY);
        return leader != null && leader.contains(identity);
    }

}
