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

package org.apache.geaflow.cluster.k8s.clustermanager;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.RetryCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Geaflow Kubernetes client to interact with kubernetes api server.
 */
public class GeaflowKubeClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaflowKubeClient.class);

    private final KubernetesMasterParam masterParam;
    private final KubernetesClient kubernetesClient;

    private final int retryCount;
    private final long retryInterval;

    public GeaflowKubeClient(Configuration config) {
        this(KubernetesClientFactory.create(config), config);
    }

    public GeaflowKubeClient(Map<String, String> config, String masterUrl) {
        this(new Configuration(config), masterUrl);
    }

    public GeaflowKubeClient(Configuration config, String masterUrl) {
        this(KubernetesClientFactory.create(config, masterUrl), config);
    }

    public GeaflowKubeClient(KubernetesClient client, Configuration config) {
        this.kubernetesClient = client;
        this.masterParam = new KubernetesMasterParam(config);
        this.retryCount = KubernetesConfig.getConnectionRetryTimes(config);
        this.retryInterval = KubernetesConfig.getConnectionRetryIntervalMs(config);
    }

    public String getKubernetesMasterHost() {
        return kubernetesClient.getMasterUrl().getHost();
    }

    public ConfigMap createOrReplaceConfigMap(ConfigMap configMap) {
        return runWithRetries(() -> kubernetesClient.configMaps().createOrReplace(configMap));
    }

    public ConfigMap updateConfigMap(ConfigMap configMap) {
        return runWithRetries(() -> kubernetesClient.resource(configMap).lockResourceVersion().replace());
    }

    public Service getService(String serviceName) {
        return runWithRetries((() -> kubernetesClient.services().withName(serviceName).get()));
    }

    public PodList getPods(Map<String, String> labels) {
        return runWithRetries((() -> kubernetesClient.pods().withLabels(labels).list()));
    }

    public ConfigMap getConfigMap(String configmapName) {
        return runWithRetries((() -> kubernetesClient.configMaps().withName(configmapName).get()));
    }

    public Deployment getDeployment(String name) {
        return runWithRetries((() -> kubernetesClient.apps().deployments().withName(name).get()));
    }

    public Service createService(Service service) {
        Callable<Service> action = () -> {
            LOGGER.info("create service: {}", service.getMetadata().getName());
            return kubernetesClient.services().create(service);
        };
        return runWithRetries(action);
    }

    public void createPod(Pod pod) {
        Callable<Void> action = () -> {
            LOGGER.info("create pod: {}", pod.getMetadata().getName());
            kubernetesClient.pods().create(pod);
            return null;
        };
        runWithRetries(action);
    }

    public ConfigMap createConfigMap(ConfigMap configMap) {
        Callable<ConfigMap> action = () -> {
            LOGGER.info("create configmap: {}", configMap.getMetadata().getName());
            return kubernetesClient.configMaps().create(configMap);
        };
        return runWithRetries(action);
    }

    public void createDeployment(Deployment deployment) {
        Callable<Void> action = () -> {
            kubernetesClient.apps().deployments().create(deployment);
            LOGGER.info("create deployment: {}", deployment.getMetadata().getName());
            return null;
        };
        runWithRetries(action);
    }

    public Watch createPodsWatcher(Map<String, String> labels,
                                   BiConsumer<Action, Pod> eventHandler,
                                   Consumer<Exception> closeHandler) {
        Callable<Watch> action = () -> {
            Watcher<Pod> watcher = createWatcher(eventHandler, closeHandler);
            PodList podList = kubernetesClient.pods().withLabels(labels).list();
            String resourceVersion = podList.getMetadata().getResourceVersion();
            LOGGER.info("create watcher for {} pods with resource version: {} labels: {}", podList.getItems().size(), resourceVersion, labels);
            return kubernetesClient.pods().withLabels(labels).withResourceVersion(resourceVersion).watch(watcher);
        };
        return runWithRetries(action);
    }

    public Watch createServiceWatcher(String serviceName,
                                      BiConsumer<Action, Service> eventHandler,
                                      Consumer<Exception> closeHandler) {
        Callable<Watch> action = () -> {
            Watcher<Service> watcher = createWatcher(eventHandler, closeHandler);
            LOGGER.info("create watcher for service with name: {}", serviceName);
            return kubernetesClient.services().withName(serviceName).watch(watcher);
        };
        return runWithRetries(action);
    }

    private <R extends HasMetadata> Watcher<R> createWatcher(BiConsumer<Action, R> eventHandler,
                                                             Consumer<Exception> closeHandler) {
        Watcher<R> watcher = new Watcher<R>() {
            @Override
            public void eventReceived(Action action, R resource) {
                eventHandler.accept(action, resource);
            }

            @Override
            public void onClose(WatcherException e) {
                if (e != null) {
                    LOGGER.warn("Watcher onClose: {}", e.getMessage());
                }
                closeHandler.accept(e);
            }
        };
        return watcher;
    }

    public LeaderElector createLeaderElector(LeaderElectionConfig config, ExecutorService service) {
        return new LeaderElector(kubernetesClient, config, service);
    }

    private <T> T runWithRetries(Callable<T> action) {
        return RetryCommand.run(action, retryCount, retryInterval);
    }

    public void destroyCluster(String clusterId) {
        String serviceName = KubernetesUtils.getMasterServiceName(clusterId);
        LOGGER.info("delete cluster with service:{}", serviceName);
        kubernetesClient.services().withName(serviceName).delete();
    }

    public void deleteConfigMap(String configMapName) {
        LOGGER.info("delete configMap:{}", configMapName);
        kubernetesClient.configMaps().withName(configMapName).delete();
    }

    public void deletePod(Map<String, String> labels) {
        Callable<Void> action = () -> {
            FilterWatchListDeletable<Pod, PodList, PodResource> running = kubernetesClient.pods().withLabels(labels);
            List<Pod> pods = running.list().getItems();
            if (!pods.isEmpty()) {
                LOGGER.info("delete {} running pod with label:{}", pods.size(),
                    labels);
                pods.forEach(pod -> kubernetesClient.resource(pod).delete());
            }
            return null;
        };
        runWithRetries(action);
    }

    public void close() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

}
