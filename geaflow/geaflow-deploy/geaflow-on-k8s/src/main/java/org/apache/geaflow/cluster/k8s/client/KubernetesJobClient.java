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

package org.apache.geaflow.cluster.k8s.client;

import static org.apache.geaflow.cluster.k8s.config.K8SConstants.CLIENT_NAME_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesResourceBuilder;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesClientParam;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to submit a job in cluster mode.
 * The config contains: clusterId, mainClass, clientArgs, and k8s related config.
 */
public class KubernetesJobClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesJobClient.class);

    private final Configuration configuration;
    private final GeaflowKubeClient geaflowKubeClient;
    private final KubernetesClientParam clientParam;
    private final String clusterId;

    public KubernetesJobClient(Map<String, String> config, String masterUrl) {
        this(config, new GeaflowKubeClient(config, masterUrl));
    }

    @VisibleForTesting
    public KubernetesJobClient(Map<String, String> config, GeaflowKubeClient client) {
        this.configuration = new Configuration(config);
        this.clientParam = new KubernetesClientParam(configuration);
        this.geaflowKubeClient = client;
        this.clusterId = configuration.getString(ExecutionConfigKeys.CLUSTER_ID);
        Preconditions.checkArgument(StringUtils.isNotBlank(clusterId),
            "ClusterId is not set: " + ExecutionConfigKeys.CLUSTER_ID);
    }

    public void submitJob() {
        try {
            DockerNetworkType dockerNetworkType = KubernetesConfig
                .getDockerNetworkType(configuration);

            // create configMap.
            ConfigMap configMap = createConfigMap(clusterId);

            // create container
            String podName = clusterId + CLIENT_NAME_SUFFIX;
            Container container = KubernetesResourceBuilder
                .createContainer(podName, podName, null,
                    clientParam, clientParam.getContainerShellCommand(), clientParam.getAdditionEnvs(),
                    dockerNetworkType);

            // create ownerReference
            OwnerReference ownerReference = createOwnerReference(configMap);

            // create pod
            Pod pod = KubernetesResourceBuilder
                .createPod(clusterId, podName, podName, ownerReference, configMap, clientParam,
                    container);
            geaflowKubeClient.createPod(pod);
        } catch (Exception e) {
            LOGGER.error("Failed to create client pod:{}", e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Setup a Config Map that will generate a geaflow-conf.yaml and log4j file.
     *
     * @param clusterId the cluster id
     * @return the created configMap
     */
    private ConfigMap createConfigMap(String clusterId) {
        ConfigMap configMap = KubernetesResourceBuilder.createConfigMap(clusterId, clientParam,
            null);
        return geaflowKubeClient.createConfigMap(configMap);
    }

    private OwnerReference createOwnerReference(ConfigMap configMap) {
        Preconditions.checkNotNull(configMap, "configMap could not be null");
        return new OwnerReferenceBuilder()
            .withName(configMap.getMetadata().getName())
            .withApiVersion(configMap.getApiVersion())
            .withUid(configMap.getMetadata().getUid())
            .withKind(configMap.getKind())
            .withController(true)
            .build();
    }

    public void stopJob() {
        String clientConfigMap = clientParam.getConfigMapName(clusterId);
        geaflowKubeClient.deleteConfigMap(clientConfigMap);
        geaflowKubeClient.destroyCluster(clusterId);
    }

    public Service getMasterService() {
        String masterServiceName = KubernetesUtils.getMasterServiceName(clusterId);
        return geaflowKubeClient.getService(masterServiceName);
    }

    public List<Pod> getJobPods() {
        Map<String, String> podLabels = new HashMap<>();
        podLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        return geaflowKubeClient.getPods(podLabels).getItems();
    }

}
