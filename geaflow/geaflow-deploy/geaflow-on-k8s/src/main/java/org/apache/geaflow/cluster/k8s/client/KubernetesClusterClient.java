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

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfig.DRIVER_EXPOSED_ADDRESS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfig.MASTER_EXPOSED_ADDRESS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType.CLUSTER_IP;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.WORK_DIR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.client.AbstractClusterClient;
import org.apache.geaflow.cluster.client.IPipelineClient;
import org.apache.geaflow.cluster.client.PipelineClientFactory;
import org.apache.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesClusterManager;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.ctx.IEnvironmentContext;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.apache.geaflow.stats.model.EventLabel;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClusterClient.class);
    private static final int DEFAULT_SLEEP_MS = 1000;

    private GeaflowKubeClient kubernetesClient;
    private KubernetesClusterManager clusterManager;
    private String clusterId;
    private int clientTimeoutMs;

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        if (!config.contains(JOB_WORK_PATH)) {
            config.put(JOB_WORK_PATH, config.getString(WORK_DIR));
        }
        if (!config.contains(CLUSTER_ID)) {
            config.put(CLUSTER_ID, K8SConstants.RANDOM_CLUSTER_ID_PREFIX + UUID.randomUUID());
        }
        this.clusterId = config.getString(CLUSTER_ID);
        String masterUrl = KubernetesConfig.getClientMasterUrl(config);
        this.kubernetesClient = new GeaflowKubeClient(config, masterUrl);
        this.clusterManager = new KubernetesClusterManager();
        this.clusterManager.init(new ClusterContext(config), kubernetesClient);
        this.clientTimeoutMs = KubernetesConfig.getClientTimeoutMs(config);
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            this.clusterId = clusterManager.startMaster().getHandler();
            Map<String, ConnectAddress> driverAddresses = waitForMasterStarted(clusterId);
            ClusterMeta clusterMeta = new ClusterMeta(driverAddresses,
                config.getString(MASTER_EXPOSED_ADDRESS));
            callback.onSuccess(clusterMeta);
            LOGGER.info("Cluster info: {} config: {}", clusterMeta, config);
            String successMsg = String.format("Start cluster success. Cluster info: %s", clusterMeta);
            StatsCollectorFactory.init(config).getEventCollector()
                .reportEvent(ExceptionLevel.INFO, EventLabel.START_CLUSTER_SUCCESS, successMsg);
            return PipelineClientFactory.createPipelineClient(driverAddresses, config);
        } catch (Throwable e) {
            LOGGER.error("Deploy failed.", e);
            callback.onFailure(e);
            String failMsg = String.format("Start cluster failed: %s", e.getMessage());
            StatsCollectorFactory.init(config).getEventCollector()
                .reportEvent(ExceptionLevel.FATAL, EventLabel.START_CLUSTER_FAILED, failMsg);
            kubernetesClient.destroyCluster(clusterId);
            throw new GeaflowRuntimeException(e);
        }
    }

    private Map<String, ConnectAddress> waitForMasterStarted(String clusterId) throws TimeoutException {
        ClusterInfo clusterInfo = waitForMasterConfigUpdated(clusterId);
        DockerNetworkType networkType = KubernetesConfig.getDockerNetworkType(config);
        if (networkType != DockerNetworkType.HOST) {
            updateServiceAddress(clusterId, clusterInfo);
        }
        return clusterInfo.getDriverAddresses();
    }

    private ClusterInfo waitForMasterConfigUpdated(String clusterId) throws TimeoutException {
        Map<String, String> configuration;
        final long startTime = System.currentTimeMillis();
        KubernetesMasterParam masterParam = new KubernetesMasterParam(config);
        while (true) {
            String configName = masterParam.getConfigMapName(clusterId);
            ConfigMap configMap = kubernetesClient.getConfigMap(configName);
            configuration = KubernetesUtils.loadConfigurationFromString(
                configMap.getData().get(K8SConstants.ENV_CONFIG_FILE));
            if (configuration.containsKey(DRIVER_EXPOSED_ADDRESS)) {
                break;
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > 60000) {
                LOGGER.warn("Start cluster took more than 60 seconds, please check logs on the "
                    + "Kubernetes cluster.");
                if (elapsedTime > clientTimeoutMs) {
                    throw new TimeoutException("Waiting cluster ready timeout.");
                }
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }

        ClusterInfo clusterInfo = new ClusterInfo();
        String driverAddress = configuration.get(DRIVER_EXPOSED_ADDRESS);
        clusterInfo.setDriverAddresses(KubernetesUtils.decodeRpcAddressMap(driverAddress));
        return clusterInfo;
    }

    @Override
    public void shutdown() {
        Preconditions.checkNotNull(clusterId, "ClusterId is null.");
        kubernetesClient.destroyCluster(clusterId);
    }

    private void updateServiceAddress(String clusterId, ClusterInfo clusterInfo)
        throws TimeoutException {
        ServiceExposedType serviceType = KubernetesConfig.getServiceExposedType(config);
        String masterServiceName;
        if (serviceType == CLUSTER_IP) {
            masterServiceName = KubernetesUtils.getMasterServiceName(clusterId);
        } else {
            masterServiceName = KubernetesUtils.getMasterClientServiceName(clusterId);
        }
        String serviceAddress = setupExposedServiceAddress(serviceType, masterServiceName,
            K8SConstants.HTTP_PORT);
        config.put(MASTER_EXPOSED_ADDRESS, serviceAddress);

        int driverIndex = 0;
        Map<String, ConnectAddress> driverAddresses = new HashMap<>();
        Map<String, ConnectAddress> originalDriverAddresses = clusterInfo.getDriverAddresses();
        for (String driverId : originalDriverAddresses.keySet()) {
            String driverServiceName = KubernetesUtils.getDriverServiceName(clusterId, driverIndex);
            ConnectAddress rpcAddress = ConnectAddress.build(setupExposedServiceAddress(serviceType,
                driverServiceName, K8SConstants.RPC_PORT));
            driverAddresses.put(driverId, rpcAddress);
            driverIndex++;
        }
        config.put(DRIVER_EXPOSED_ADDRESS, KubernetesUtils.encodeRpcAddressMap(driverAddresses));
        clusterInfo.setDriverAddresses(driverAddresses);
    }

    private String setupExposedServiceAddress(ServiceExposedType serviceType, String serviceName,
                                              String portName) throws TimeoutException {
        String serviceAddress = "localhost";
        switch (serviceType) {
            case CLUSTER_IP:
                serviceAddress = resolveServiceExposedByClusterIp(serviceName, portName);
                break;
            case NODE_PORT:
                serviceAddress = resolveServiceExposedByNodePort(serviceName, portName);
                break;
            case LOAD_BALANCER:
                serviceAddress = resolveServiceExposedByLoadBalancer(serviceName);
                break;
            default:
                break;
        }

        LOGGER.info("Service {} exposed: {}.", serviceName, serviceAddress);
        return serviceAddress;
    }

    private String resolveServiceExposedByClusterIp(String serviceName, String portName)
        throws TimeoutException {
        String namespace = config.getString(NAME_SPACE);
        String serviceSuffix = config.getString(SERVICE_SUFFIX);
        String serviceAddress = serviceName + K8SConstants.NAMESPACE_SEPARATOR + namespace;
        if (!StringUtils.isBlank(serviceSuffix)) {
            serviceAddress += K8SConstants.NAMESPACE_SEPARATOR + serviceSuffix;
        }

        LOGGER.info("Waiting for service {} to be exposed by cluster ip.", serviceName);
        Service service = getService(serviceName);

        int port = 0;
        for (ServicePort servicePort : service.getSpec().getPorts()) {
            if (servicePort.getName().equals(portName)) {
                port = servicePort.getPort();
                break;
            }
        }
        return new ConnectAddress(serviceAddress, port).toString();
    }

    private String resolveServiceExposedByNodePort(String serviceName, String portName)
        throws TimeoutException {
        LOGGER.info("Waiting for service {} to be exposed by node port.", serviceName);
        Service service = getService(serviceName);

        int nodePort = 0;
        for (ServicePort servicePort : service.getSpec().getPorts()) {
            if (servicePort.getName().equals(portName)) {
                nodePort = servicePort.getNodePort();
                break;
            }
        }
        return new ConnectAddress(kubernetesClient.getKubernetesMasterHost(), nodePort).toString();
    }

    private String resolveServiceExposedByLoadBalancer(String serviceName) {
        LOGGER.info("Waiting for service {} to be exposed by load balancer.", serviceName);
        List<String> ipList = new ArrayList<>();
        List<LoadBalancerIngress> ingressList;
        final long startTime = System.currentTimeMillis();
        Service service;
        while (true) {
            service = kubernetesClient.getService(serviceName);
            if (service != null) {
                ingressList = service.getStatus().getLoadBalancer().getIngress();
                if (ingressList != null && ingressList.size() > 0) {
                    for (LoadBalancerIngress ingress : ingressList) {
                        ipList.add(ingress.getIp());
                    }
                    break;
                }
            }
            if (System.currentTimeMillis() - startTime > 60000) {
                LOGGER.warn("Expose service took more than 60s, please check logs on the "
                    + "Kubernetes cluster.");
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }
        return ipList.get(0);
    }

    private Service getService(String serviceName) throws TimeoutException {
        Service service = kubernetesClient.getService(serviceName);
        final long startTime = System.currentTimeMillis();
        while (service == null) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > 60000) {
                LOGGER.warn("Get service {} took more than 60s, please check logs on Kubernetes"
                    + " cluster.", serviceName);
                if (elapsedTime > clientTimeoutMs) {
                    throw new TimeoutException("Resolve service " + serviceName + " timeout.");
                }
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
            service = kubernetesClient.getService(serviceName);
        }
        return service;
    }

}
