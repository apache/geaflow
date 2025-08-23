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

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.WATCHER_CHECK_INTERVAL;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.ha.leaderelection.ILeaderContender;
import org.apache.geaflow.ha.leaderelection.ILeaderElectionEventListener;
import org.apache.geaflow.ha.leaderelection.ILeaderElectionService;
import org.apache.geaflow.ha.leaderelection.LeaderContenderType;
import org.apache.geaflow.ha.leaderelection.LeaderElectionServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesLeaderElectionService implements ILeaderElectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesLeaderElectionService.class);

    private static final String NAME_SEPERATOR = "-";

    private GeaflowKubeClient client;

    private KubernetesLeaderElectorConfig electorConfig;

    private KubernetesLeaderElector leaderElector;

    private Configuration configuration;

    private String componentId;

    private String identity;

    private ILeaderContender contender;

    private Watch masterServiceWatcher;

    private volatile boolean watcherClosed;

    private final ScheduledExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor(
            ThreadUtil.namedThreadFactory(true, "watcher-creator"));

    public void init(Configuration configuration, String componentId) {
        this.configuration = configuration;
        this.componentId = componentId;
        this.client = new GeaflowKubeClient(configuration);
        this.identity = UUID.randomUUID().toString();
        this.watcherClosed = true;
        LOGGER.info("Init leader-election service with identity: {}", identity);
    }

    @Override
    public void open(ILeaderContender contender) {
        this.contender = contender;
        KubernetesLeaderElectionEventListener eventListener =
            new KubernetesLeaderElectionEventListener();
        initMasterServiceWatcher();
        String configMapName = getLeaderConfigMapName(contender);
        electorConfig = KubernetesLeaderElectorConfig.build(configuration, configMapName, identity);
        LeaderCallbacks callbacks = new LeaderCallbacks(
            eventListener::handleLeadershipGranted,
            eventListener::handleLeadershipLost,
            eventListener::handleNewLeadership
        );
        leaderElector = new KubernetesLeaderElector(client, electorConfig, callbacks);
        leaderElector.run();
    }

    private void initMasterServiceWatcher() {
        String clusterId = configuration.getString(ExecutionConfigKeys.CLUSTER_ID);
        String masterServiceName = KubernetesUtils.getMasterServiceName(clusterId);
        BiConsumer<Action, Service> eventHandler = this::handleClusterDestroyed;
        Consumer<Exception> exceptionHandler = (exception) -> {
            watcherClosed = true;
            LOGGER.warn("watch exception: {}", exception.getMessage(), exception);
        };
        int checkInterval = configuration.getInteger(WATCHER_CHECK_INTERVAL);
        executorService.scheduleAtFixedRate(() -> {
            if (watcherClosed) {
                if (masterServiceWatcher != null) {
                    masterServiceWatcher.close();
                }
                masterServiceWatcher = client.createServiceWatcher(masterServiceName, eventHandler, exceptionHandler);
                if (masterServiceWatcher != null) {
                    watcherClosed = false;
                }
            }
        }, 0, checkInterval, TimeUnit.SECONDS);
    }

    private void handleClusterDestroyed(Action action, Service service) {
        // Leader-election service should be stopped if the cluster is destroyed,
        // to avoid creating the config-map-lock again after deleted.
        if (action == Action.DELETED) {
            LOGGER.warn("Master service {} is deleted, close the leader-election service now.",
                service.getMetadata().getName());
            leaderElector.close();
        }
    }

    @Override
    public void close() {
        if (leaderElector != null) {
            leaderElector.close();
        }
        executorService.shutdownNow();
    }

    @Override
    public boolean isLeader() {
        ConfigMap configMap = client.getConfigMap(electorConfig.getConfigMapName());
        return KubernetesLeaderElector.isLeader(configMap, electorConfig.getIdentity());
    }

    @Override
    public LeaderElectionServiceType getType() {
        return LeaderElectionServiceType.kubernetes;
    }

    private class KubernetesLeaderElectionEventListener implements ILeaderElectionEventListener {

        @Override
        public void handleLeadershipGranted() {
            String clusterId = configuration.getString(ExecutionConfigKeys.CLUSTER_ID);
            Map<String, String> configMapLabels = new HashMap<>();
            configMapLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
            configMapLabels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, componentId);
            configMapLabels.put(K8SConstants.LABEL_CONFIG_MAP_LOCK, Boolean.toString(true));
            configMapLabels.putAll(KubernetesUtils.getPairsConf(configuration, POD_USER_LABELS));
            ConfigMap configMap = client.getConfigMap(getLeaderConfigMapName(contender));
            configMap.getMetadata().setLabels(configMapLabels);

            Service masterService = client.getService(KubernetesUtils.getMasterServiceName(clusterId));
            configMap.addOwnerReference(masterService);
            client.updateConfigMap(configMap);
            contender.handleLeadershipGranted();
        }

        @Override
        public void handleLeadershipLost() {
            contender.handleLeadershipLost();
        }

        @Override
        public void handleNewLeadership(String newLeader) {
            LOGGER.info("New leader for contender {} is elected. The leader is {}.",
                contender.getClass().getSimpleName(), newLeader.equals(identity) ? "me" : newLeader);
        }
    }

    private String getLeaderConfigMapName(ILeaderContender contender) {
        String clusterId = configuration.getString(ExecutionConfigKeys.CLUSTER_ID);
        LeaderContenderType contenderType = contender.getType();
        return clusterId + NAME_SEPERATOR + contenderType + NAME_SEPERATOR + componentId;
    }
}
