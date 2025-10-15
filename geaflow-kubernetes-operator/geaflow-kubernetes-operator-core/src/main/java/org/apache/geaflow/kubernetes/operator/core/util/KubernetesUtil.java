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

package org.apache.geaflow.kubernetes.operator.core.util;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.kubernetes.operator.core.model.constants.DeploymentConstants;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractGeaflowResource;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.core.model.exception.GeaflowDeploymentException;
import org.apache.geaflow.kubernetes.operator.core.model.job.ComponentType;

@Slf4j
public class KubernetesUtil {

    private static KubernetesClient kubernetesClient;

    public static KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }

    public static void setKubernetesClient(KubernetesClient kubernetesClient) {
        KubernetesUtil.kubernetesClient = kubernetesClient;
    }

    public static void createGeaflowJob(GeaflowJob geaflowJob) {
        kubernetesClient.resource(geaflowJob).create();
    }

    public static void deleteGeaflowJob(String appId) {
        kubernetesClient.resources(GeaflowJob.class).withName(appId).delete();
    }

    public static Collection<GeaflowJob> getGeaflowJobs(boolean usingCache) {
        ListOptions options = new ListOptions();
        if (usingCache) {
            options.setResourceVersion("0");
        }
        return kubernetesClient.resources(GeaflowJob.class).list(options).getItems();
    }

    public static String getMasterServiceName(String appId) {
        return appId + "-service";
    }

    public static ConfigMap getMasterConfigMap(String appId) {
        String masterConfigMapName = appId + K8SConstants.MASTER_CONFIG_MAP_SUFFIX;
        return getConfigMap(masterConfigMapName);
    }

    public static ConfigMap getConfigMap(String configMapName) {
        return kubernetesClient.configMaps().withName(configMapName).get();
    }

    public static PodList getMasterPodList(String appId) {
        return getPodList(appId, K8SConstants.LABEL_COMPONENT_MASTER);
    }

    public static PodList getClientPodList(String appId) {
        return getPodList(appId, K8SConstants.LABEL_COMPONENT_CLIENT);
    }

    private static PodList getPodList(String appId, String componentLabel) {
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, appId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, componentLabel);
        return getPodList(labels);
    }

    private static PodList getPodList(Map<String, String> labels) {
        return kubernetesClient.pods().withLabels(labels).list();
    }

    public static Service getMasterService(String appId) {
        return getService(getMasterServiceName(appId));
    }

    public static Service getService(String serviceName) {
        return kubernetesClient.services().withName(serviceName).get();
    }

    public static Deployment getMasterDeployment(String appId) {
        String name = appId + K8SConstants.MASTER_RS_NAME_SUFFIX;
        return getDeployment(name);
    }

    public static Deployment getDeployment(String name) {
        return kubernetesClient.apps().deployments().withName(name).get();
    }

    public static void checkMasterPodBackoff(String appId) {
        PodList masterPodList = KubernetesUtil.getMasterPodList(appId);
        checkPodBackOff(masterPodList.getItems(), ComponentType.master);
    }

    public static void checkClientPodBackoff(String appId) {
        PodList masterPodList = KubernetesUtil.getClientPodList(appId);
        checkPodBackOff(masterPodList.getItems(), ComponentType.client);
    }

    public static void checkPodBackOff(Collection<Pod> podList, ComponentType componentType) {
        for (Pod pod : podList) {
            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                ContainerStateWaiting csw = cs.getState().getWaiting();
                if (csw != null && Set.of(DeploymentConstants.CRASH_LOOP_BACKOFF,
                        DeploymentConstants.IMAGE_PULL_BACKOFF, DeploymentConstants.ERR_IMAGE_PULL)
                    .contains(csw.getReason())) {
                    throw new GeaflowDeploymentException(csw, componentType);
                }
            }
        }
    }

    public static boolean isPodStateRunning(Collection<Pod> podList) {
        for (Pod pod : podList) {
            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                ContainerStateWaiting csw = cs.getState().getWaiting();
                ContainerStateRunning csr = cs.getState().getRunning();
                if (csw != null && csr == null) {
                    return false;
                }
            }
        }
        return true;
    }

    public static void deleteClientConfigMap(String appId) {
        String clientConfigMapName = appId + K8SConstants.CLIENT_CONFIG_MAP_SUFFIX;
        deleteConfigMap(clientConfigMapName);
    }

    public static void deleteConfigMap(String configMapName) {
        kubernetesClient.configMaps().withName(configMapName).delete();
    }

    public static void deleteMasterService(String appId) {
        if (StringUtils.isBlank(appId)) {
            return;
        }
        String masterServiceName = getMasterServiceName(appId);
        deleteService(masterServiceName);
    }

    public static <R extends AbstractGeaflowResource> void deleteMasterService(Context<R> context) {
        context.getSecondaryResource(Service.class).ifPresent(KubernetesUtil::deleteService);
    }

    public static void deleteService(String serviceName) {
        log.info("Delete service with name {}.", serviceName);
        kubernetesClient.services().withName(serviceName).delete();
    }

    public static void deleteService(Service service) {
        log.info("Delete service with name {}.", service.getMetadata().getName());
        kubernetesClient.resource(service).delete();
    }

    public static void deleteService(Map<String, String> labels) {
        kubernetesClient.services().withLabels(labels).delete();
    }

    public static Configuration generateKubernetesConfig() {
        Configuration configuration = new Configuration();
        Config config = kubernetesClient.getConfiguration();
        configuration.put(KubernetesConfigKeys.MASTER_URL, config.getMasterUrl());
        configuration.put(KubernetesConfigKeys.NAME_SPACE, config.getNamespace());
        configuration.put(KubernetesConfigKeys.PING_INTERVAL_MS,
            String.valueOf(config.getWebsocketPingInterval()));
        configuration.put(KubernetesConfigKeys.CERT_KEY, config.getClientKeyData());
        configuration.put(KubernetesConfigKeys.CERT_DATA, config.getClientCertData());
        configuration.put(KubernetesConfigKeys.CA_DATA, config.getCaCertData());
        return configuration;
    }

    public static void waitForClusterDestroyed(String appId, int destroyTimeoutSeconds) {
        boolean clientRunning = true;
        boolean masterRunning = true;

        for (int i = 0; i < destroyTimeoutSeconds; i++) {
            if (clientRunning) {
                PodList clientPodList = getClientPodList(appId);
                if (clientPodList == null || clientPodList.getItems().isEmpty()) {
                    clientRunning = false;
                }
            }

            if (masterRunning) {
                Service masterService = getMasterService(appId);
                Deployment masterDeployment = getMasterDeployment(appId);
                if (masterService == null && masterDeployment == null) {
                    PodList masterPodList = getMasterPodList(appId);
                    if (masterPodList == null || masterPodList.getItems().isEmpty()) {
                        masterRunning = false;
                    }
                }
            }

            if (!masterRunning && !clientRunning) {
                break;
            }

            if ((i + 1) % 5 == 0) {
                log.info("Waiting for cluster {} destroy. {}s", appId, i + 1);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("Cluster destroyed successfully. {}", appId);
    }

}
