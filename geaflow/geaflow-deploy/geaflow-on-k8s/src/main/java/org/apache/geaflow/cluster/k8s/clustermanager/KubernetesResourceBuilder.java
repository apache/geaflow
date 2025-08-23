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

import static org.apache.geaflow.cluster.k8s.config.K8SConstants.CONFIG_KV_SEPARATOR;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.CONFIG_LIST_SEPARATOR;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.LABEL_COMPONENT_ID_KEY;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.ALWAYS_PULL_ENGINE_JAR;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CLUSTER_NAME;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_CONF_FILES;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DNS_SEARCH_DOMAINS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.ENGINE_JAR_FILES;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.USER_JAR_FILES;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.WORK_DIR;
import static org.apache.geaflow.common.config.keys.DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT;

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.ObjectFieldSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import org.apache.geaflow.cluster.k8s.config.KubernetesParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesResourceBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesResourceBuilder.class);

    public static Container createContainer(String containerName, String containerId,
                                            String masterId, KubernetesParam param, String command,
                                            Map<String, String> additionalEnvs,
                                            DockerNetworkType dockerNetworkType) {

        Quantity masterCpuQuantity = param.getCpuQuantity();
        Quantity masterMemoryQuantity = param.getMemoryQuantity();

        String image = param.getContainerImage();
        Preconditions.checkNotNull(image, "container image should be specified");

        Configuration config = param.getConfig();
        String clusterName = config.getString(CLUSTER_NAME);
        String pullPolicy = param.getContainerImagePullPolicy();
        String confDir = param.getConfDir();
        String logDir = param.getLogDir();
        String jobWorkPath = config.getString(WORK_DIR);
        String jarDownloadPath = KubernetesConfig.getJarDownloadPath(config);
        String udfList = config.getString(USER_JAR_FILES);
        String engineJar = config.getString(ENGINE_JAR_FILES);
        String autoRestart = param.getAutoRestart();
        String gatewayEndpoint = config.getString(GEAFLOW_GW_ENDPOINT, "");
        String geaflowToken = config.getString(GEAFLOW_DSL_CATALOG_TOKEN_KEY, "");
        Boolean clusterFaultInjectionEnable = param.getClusterFaultInjectionEnable();
        boolean alwaysDownloadEngineJar = config.getBoolean(ALWAYS_PULL_ENGINE_JAR);

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(containerName)
            .withImage(image)
            .withImagePullPolicy(pullPolicy)
            .addNewEnv()
            .withName(K8SConstants.ENV_CONF_DIR).withValue(confDir).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_LOG_DIR).withValue(logDir).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_JOB_WORK_PATH).withValue(jobWorkPath).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_JAR_DOWNLOAD_PATH).withValue(jarDownloadPath).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_UDF_LIST).withValue(udfList).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_ENGINE_JAR).withValue(engineJar).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_START_COMMAND).withValue(command).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_CONTAINER_ID).withValue(containerId).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_CLUSTER_ID).withValue(clusterName)
            .endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_MASTER_ID).withValue(masterId).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_AUTO_RESTART).withValue(autoRestart).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_CLUSTER_FAULT_INJECTION_ENABLE).withValue(String.valueOf(clusterFaultInjectionEnable)).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_CATALOG_TOKEN).withValue(geaflowToken).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_GW_ENDPOINT).withValue(gatewayEndpoint).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_ALWAYS_DOWNLOAD_ENGINE).withValue(String.valueOf(alwaysDownloadEngineJar)).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_NODE_NAME).withValueFrom(new EnvVarSourceBuilder()
                .withFieldRef(
                    new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.NODE_NAME_FIELD_PATH)
                        .build()).build()).endEnv().addNewEnv().withName(K8SConstants.ENV_POD_NAME)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(
                new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.POD_NAME_FIELD_PATH)
                    .build()).build()).endEnv().addNewEnv().withName(K8SConstants.ENV_POD_IP)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(
                    new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.POD_IP_FIELD_PATH).build())
                .build()).endEnv().addNewEnv().withName(K8SConstants.ENV_HOST_IP)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(
                new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.HOST_IP_FIELD_PATH)
                    .build()).build()).endEnv()
            .addNewEnv()
            .withName(K8SConstants.ENV_SERVICE_ACCOUNT).withValueFrom(
                new EnvVarSourceBuilder().withFieldRef(
                    new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.SERVICE_ACCOUNT_NAME_FIELD_PATH)
                        .build()).build())
            .endEnv()
            .addNewEnv().withName(K8SConstants.ENV_NAMESPACE).withValueFrom(
                new EnvVarSourceBuilder().withFieldRef(
                    new ObjectFieldSelectorBuilder().withFieldPath(K8SConstants.NAMESPACE_FIELD_PATH)
                        .build()).build()).endEnv()
            .editOrNewResources()
            .addToRequests(K8SConstants.RESOURCE_NAME_MEMORY, masterMemoryQuantity)
            .addToRequests(K8SConstants.RESOURCE_NAME_CPU, masterCpuQuantity).endResources()
            .withVolumeMounts(new VolumeMountBuilder()
                .withName(K8SConstants.GEAFLOW_CONF_VOLUME)
                .withMountPath(confDir)
                .build());

        if (dockerNetworkType == DockerNetworkType.BRIDGE) {
            if (param.getRpcPort() > 0) {
                containerBuilder.addNewPort()
                    .withName(K8SConstants.RPC_PORT)
                    .withContainerPort(param.getRpcPort())
                    .withProtocol("TCP").endPort();
            }
            if (param.getHttpPort() > 0) {
                containerBuilder.addNewPort()
                    .withName(K8SConstants.HTTP_PORT)
                    .withContainerPort(param.getHttpPort())
                    .withProtocol("TCP").endPort();
            }
        }

        Quantity containerDiskQuantity = param.getDiskQuantity();
        if (containerDiskQuantity != null) {
            containerBuilder.editResources()
                .addToRequests(K8SConstants.RESOURCE_NAME_EPHEMERAL_STORAGE, containerDiskQuantity).endResources();
        }

        boolean enableMemoryLimit = KubernetesConfig.enableResourceMemoryLimit(config);
        boolean enableCpuLimit = KubernetesConfig.enableResourceCpuLimit(config);
        boolean enableDiskLimit = KubernetesConfig.enableResourceEphemeralStorageLimit(config);

        if (enableMemoryLimit) {
            containerBuilder.editResources()
                .addToLimits(K8SConstants.RESOURCE_NAME_MEMORY, masterMemoryQuantity).endResources();
        }
        if (enableCpuLimit) {
            containerBuilder.editResources()
                .addToLimits(K8SConstants.RESOURCE_NAME_CPU, masterCpuQuantity).endResources();
        }
        if (enableDiskLimit) {
            String size = KubernetesConfig.getResourceEphemeralStorageSize(config);
            Quantity ephemeralStorageQuantity = new Quantity(size);
            containerBuilder.editResources()
                .addToLimits(K8SConstants.RESOURCE_NAME_EPHEMERAL_STORAGE, ephemeralStorageQuantity)
                .endResources();
        }

        if (additionalEnvs != null && !additionalEnvs.isEmpty()) {
            additionalEnvs.entrySet().stream().forEach(
                e -> containerBuilder.addNewEnv().withName(e.getKey()).withValue(e.getValue())
                    .endEnv());
        }

        return containerBuilder.build();
    }

    /**
     * Setup a Config Map that will generate a conf file.
     *
     * @param clusterId the cluster id
     * @return the created configMap
     */
    public static ConfigMap createConfigMap(String clusterId, KubernetesParam param,
                                            OwnerReference ownerReference) {
        Map<String, String> config = param.getConfig().getConfigMap();
        StringBuilder confContent = new StringBuilder();
        config.forEach(
            (k, v) -> confContent.append(k).append(CONFIG_KV_SEPARATOR).append(v).append(System.lineSeparator()));

        String configMapName = param.getConfigMapName(clusterId);
        ObjectMetaBuilder metaBuilder = new ObjectMetaBuilder().withName(configMapName);
        if (ownerReference != null) {
            metaBuilder.withOwnerReferences(ownerReference);
        }

        ConfigMapBuilder configMapBuilder = new ConfigMapBuilder()
            .withMetadata(metaBuilder.build())
            .addToData(K8SConstants.ENV_CONFIG_FILE, confContent.toString());

        String files = param.getConfig().getString(CONTAINER_CONF_FILES);
        if (StringUtils.isNotEmpty(files)) {
            for (String filePath : files.split(CONFIG_LIST_SEPARATOR)) {
                String fileName = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
                String fileContent = FileUtil.getContentFromFile(filePath);
                if (fileContent != null) {
                    configMapBuilder.addToData(fileName, fileContent);
                } else {
                    LOGGER.info("File {} not exist, will not add to configMap", filePath);
                }
            }
        }

        return configMapBuilder.build();
    }

    public static ConfigMap updateConfigMap(ConfigMap configMap,
                                            Map<String, String> newConfig) {
        Map<String, String> updatedConfig = KubernetesUtils.loadConfigurationFromString(
            configMap.getData().get(K8SConstants.ENV_CONFIG_FILE));
        updatedConfig.putAll(newConfig);

        StringBuilder confContent = new StringBuilder();
        updatedConfig.forEach((k, v) ->
            confContent.append(k).append(": ").append(v).append(System.lineSeparator()));

        ConfigMapBuilder configMapBuilder = new ConfigMapBuilder()
            .withNewMetadata()
            .withName(configMap.getMetadata().getName())
            .withOwnerReferences(configMap.getMetadata().getOwnerReferences())
            .endMetadata()
            .addToData(configMap.getData());
        configMapBuilder.addToData(K8SConstants.ENV_CONFIG_FILE, confContent.toString());
        return configMapBuilder.build();
    }

    private static Map<String, String> getAnnotations(KubernetesParam param) {
        return param.getAnnotations();
    }

    public static Pod createPod(String clusterId, String name,
                                String id, OwnerReference ownerReference,
                                ConfigMap configMap, KubernetesParam param,
                                Container... container) {

        List<KeyToPath> configMapItems = configMap.getData().keySet().stream()
            .map(e -> new KeyToPath(e, null, e)).collect(Collectors.toList());

        Map<String, String> labels = param.getPodLabels(clusterId);
        labels.put(LABEL_COMPONENT_ID_KEY, id);
        Map<String, String> annotations = getAnnotations(param);

        ObjectMetaBuilder metaBuilder = new ObjectMetaBuilder().withName(name)
            .withLabels(labels).withAnnotations(annotations);
        if (ownerReference != null) {
            metaBuilder.withOwnerReferences(ownerReference);
        }
        PodBuilder podBuilder = new PodBuilder()
            .withMetadata(metaBuilder.build())
            .editOrNewSpec()
            .withServiceAccountName(param.getServiceAccount())
            .withRestartPolicy(K8SConstants.POD_RESTART_POLICY)
            .addToNodeSelector(param.getNodeSelector())
            .addToContainers(container)
            .withHostAliases(KubernetesUtils.getHostAliases(configMap))
            .addNewVolume()
            .withName(K8SConstants.GEAFLOW_CONF_VOLUME)
            .withNewConfigMap()
            .withName(param.getConfigMapName(clusterId))
            .addAllToItems(configMapItems).endConfigMap().endVolume()
            .addNewVolume()
            .withName(K8SConstants.GEAFLOW_LOG_VOLUME)
            .withNewEmptyDir().endEmptyDir()
            .endVolume()
            .endSpec();

        List<NodeSelectorRequirement> matchExpressionsList =
            KubernetesUtils.getMatchExpressions(param.getConfig());
        if (matchExpressionsList.size() > 0) {
            podBuilder.editSpec()
                .withNewAffinity()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addNewNodeSelectorTerm()
                .withMatchExpressions(matchExpressionsList)
                .endNodeSelectorTerm()
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .endAffinity()
                .endSpec();
        }

        List<Toleration> tolerationList = KubernetesUtils.getTolerations(param.getConfig());
        if (tolerationList.size() > 0) {
            podBuilder.editSpec()
                .withTolerations(tolerationList)
                .endSpec();
        }

        DockerNetworkType dockerNetworkType =
            KubernetesConfig.getDockerNetworkType(param.getConfig());
        if (dockerNetworkType == DockerNetworkType.HOST) {
            podBuilder.editSpec()
                .withHostNetwork(true)
                .withDnsPolicy(K8SConstants.HOST_NETWORK_DNS_POLICY)
                .endSpec();
        }

        return podBuilder.build();
    }

    public static Deployment createDeployment(String clusterId,
                                              String rcName,
                                              String id,
                                              Container container,
                                              ConfigMap configMap,
                                              KubernetesParam param,
                                              DockerNetworkType dockerNetworkType) {
        String configMapName = param.getConfigMapName(clusterId);
        String serviceAccount = param.getServiceAccount();

        Map<String, String> labels = param.getPodLabels(clusterId);
        labels.put(LABEL_COMPONENT_ID_KEY, id);
        Map<String, String> annotations = getAnnotations(param);

        List<KeyToPath> configMapItems = configMap.getData().keySet().stream()
            .map(e -> new KeyToPath(e, null, e)).collect(Collectors.toList());

        int replicas = param.enableLeaderElection() ? 2 : 1;

        DeploymentBuilder deploymentBuilder = new DeploymentBuilder()
            .editOrNewMetadata()
            .withName(rcName)
            .withLabels(labels)
            .endMetadata()
            .editOrNewSpec()
            .withReplicas(replicas)
            .withNewSelector()
            .addToMatchLabels(labels)
            .endSelector()
            .withNewTemplate()
            .editOrNewMetadata()
            .withLabels(labels)
            .withAnnotations(annotations)
            .endMetadata()
            .editOrNewSpec()
            .withServiceAccountName(serviceAccount)
            .withHostAliases(KubernetesUtils.getHostAliases(configMap))
            .addToContainers(container)
            .addToNodeSelector(param.getNodeSelector())
            .addNewVolume()
            .withName(K8SConstants.GEAFLOW_CONF_VOLUME)
            .withNewConfigMap()
            .withName(configMapName)
            .addAllToItems(configMapItems)
            .endConfigMap().endVolume()
            .addNewVolume()
            .withName(K8SConstants.GEAFLOW_LOG_VOLUME)
            .withNewEmptyDir()
            .endEmptyDir().endVolume()
            .endSpec()
            .endTemplate()
            .endSpec();

        String dnsDomains = param.getConfig().getString(DNS_SEARCH_DOMAINS);
        if (!StringUtils.isEmpty(dnsDomains)) {
            List<String> domains = Arrays.stream(dnsDomains.split(",")).map(String::trim)
                .collect(Collectors.toList());
            deploymentBuilder.editSpec().editTemplate().editSpec()
                .withDnsConfig(new PodDNSConfigBuilder().withSearches(domains).build()).endSpec()
                .endTemplate().endSpec();
        }

        if (dockerNetworkType == DockerNetworkType.HOST) {
            deploymentBuilder.editSpec().editTemplate()
                .editSpec()
                .withHostNetwork(true)
                .withDnsPolicy(K8SConstants.HOST_NETWORK_DNS_POLICY)
                .endSpec().endTemplate().endSpec();
        }

        List<NodeSelectorRequirement> matchExpressionsList =
            KubernetesUtils.getMatchExpressions(param.getConfig());
        if (matchExpressionsList.size() > 0) {
            deploymentBuilder.editSpec().editTemplate().editSpec()
                .withNewAffinity()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addNewNodeSelectorTerm()
                .withMatchExpressions(matchExpressionsList)
                .endNodeSelectorTerm()
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .endAffinity()
                .endSpec().endTemplate().endSpec();
        }

        List<Toleration> tolerationList = KubernetesUtils.getTolerations(param.getConfig());
        if (tolerationList.size() > 0) {
            deploymentBuilder.editSpec().editTemplate().editSpec()
                .withTolerations(tolerationList)
                .endSpec().endTemplate().endSpec();
        }

        return deploymentBuilder.build();
    }

    public static Service createService(String serviceName, ServiceExposedType exposedType,
                                        Map<String, String> labels, OwnerReference ownerReference,
                                        KubernetesParam param) {
        ObjectMetaBuilder metaBuilder = new ObjectMetaBuilder().withName(serviceName);
        if (ownerReference != null) {
            metaBuilder.withOwnerReferences(ownerReference);
        }
        Map<String, String> serviceLabels = param.getServiceLabels();
        serviceLabels.putAll(labels);
        metaBuilder.withLabels(serviceLabels);
        Map<String, String> serviceAnnotations = param.getServiceAnnotations();
        if (serviceAnnotations != null) {
            metaBuilder.withAnnotations(serviceAnnotations);
        }
        ServiceBuilder svcBuilder = new ServiceBuilder()
            .withMetadata(metaBuilder.build())
            .withNewSpec()
            .withType(exposedType.getServiceExposedType())
            .withSelector(labels)
            .endSpec();

        List<ServicePort> servicePorts = new ArrayList<>();
        if (param.getRpcPort() > 0) {
            ServicePortBuilder portBuilder = new ServicePortBuilder();
            portBuilder.withName(K8SConstants.RPC_PORT)
                .withPort(param.getRpcPort())
                .withProtocol(K8SConstants.TCP_PROTOCOL);
            if (exposedType == ServiceExposedType.NODE_PORT && param.getNodePort() > 0) {
                portBuilder.withNodePort(param.getNodePort());
            }
            servicePorts.add(portBuilder.build());
        }
        if (param.getHttpPort() > 0) {
            ServicePortBuilder portBuilder = new ServicePortBuilder();
            portBuilder.withName(K8SConstants.HTTP_PORT)
                .withPort(param.getHttpPort())
                .withProtocol(K8SConstants.TCP_PROTOCOL);
            if (exposedType == ServiceExposedType.NODE_PORT && param.getNodePort() > 0) {
                portBuilder.withNodePort(param.getNodePort());
            }
            servicePorts.add(portBuilder.build());
        }
        svcBuilder.editSpec().addAllToPorts(servicePorts).endSpec();

        if (exposedType.equals(ServiceExposedType.CLUSTER_IP)) {
            svcBuilder.editSpec().withClusterIP("None").endSpec();
        }

        return svcBuilder.build();
    }

    public static OwnerReference createOwnerReference(Service service) {
        Preconditions.checkNotNull(service, "Service is required to create owner reference.");
        return new OwnerReferenceBuilder()
            .withName(service.getMetadata().getName())
            .withApiVersion(service.getApiVersion())
            .withUid(service.getMetadata().getUid())
            .withKind(service.getKind())
            .withController(true)
            .build();
    }

}
