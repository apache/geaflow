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

package org.apache.geaflow.cluster.k8s.watcher;

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.WATCHER_CHECK_INTERVAL;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.handler.IPodEventHandler;
import org.apache.geaflow.cluster.k8s.handler.PodHandlerRegistry;
import org.apache.geaflow.cluster.k8s.handler.PodHandlerRegistry.EventKind;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesPodWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesPodWatcher.class);

    private Watch watcher;
    private final int checkInterval;
    private volatile boolean watcherClosed;
    private final Map<Action, Map<EventKind, IPodEventHandler>> eventHandlerMap;
    private final GeaflowKubeClient kubernetesClient;
    private final Map<String, String> labels;
    private final ScheduledExecutorService executorService;

    public KubernetesPodWatcher(Configuration config) {
        this.watcherClosed = true;
        this.checkInterval = config.getInteger(WATCHER_CHECK_INTERVAL);
        this.kubernetesClient = new GeaflowKubeClient(config);

        this.labels = new HashMap<>();
        this.labels.put(K8SConstants.LABEL_APP_KEY, config.getString(CLUSTER_ID));

        this.executorService = Executors.newSingleThreadScheduledExecutor(
            ThreadUtil.namedThreadFactory(true, "cluster-watcher"));

        PodHandlerRegistry registry = PodHandlerRegistry.getInstance(config);
        this.eventHandlerMap = registry.getHandlerMap();
    }

    public void start() {
        createAndStartPodsWatcher();
    }

    public void close() {
        executorService.shutdown();
    }

    private void createAndStartPodsWatcher() {
        BiConsumer<Action, Pod> eventHandler = this::handlePodMessage;
        Consumer<Exception> exceptionHandler = (exception) -> {
            watcherClosed = true;
            LOGGER.warn("watch exception: {}", exception.getMessage(), exception);
        };

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            ThreadUtil.namedThreadFactory(true, "watcher-creator"));
        executorService.scheduleAtFixedRate(() -> {
            if (watcherClosed) {
                if (watcher != null) {
                    watcher.close();
                }
                watcher = kubernetesClient.createPodsWatcher(labels, eventHandler,
                    exceptionHandler);
                if (watcher != null) {
                    watcherClosed = false;
                }
            }
        }, 0, checkInterval, TimeUnit.SECONDS);
    }

    private void handlePodMessage(Watcher.Action action, Pod pod) {
        String componentId = KubernetesUtils.extractComponentId(pod);
        if (componentId == null) {
            LOGGER.warn("Unknown pod {} with labels:{} event:{}", pod.getMetadata().getName(),
                pod.getMetadata().getLabels(), action);
            return;
        }
        String component = KubernetesUtils.extractComponent(pod);
        if (K8SConstants.LABEL_COMPONENT_CLIENT.equals(component)) {
            return;
        }
        if (eventHandlerMap.containsKey(action)) {
            eventHandlerMap.get(action).forEach((kind, handler) -> handler.handle(pod));
        } else {
            LOGGER.info("Skip {} event for pod {}", action, pod.getMetadata().getName());
        }
    }

}
