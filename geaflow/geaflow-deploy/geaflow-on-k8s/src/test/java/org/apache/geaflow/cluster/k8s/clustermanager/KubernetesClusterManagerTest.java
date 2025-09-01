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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.LABEL_COMPONENT_ID_KEY;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.MASTER_RS_NAME_SUFFIX;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.SERVICE_NAME_SUFFIX;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfig.CLUSTER_START_TIME;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DOCKER_NETWORK_TYPE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.MATCH_EXPRESSION_LIST;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam.MASTER_ENV_PREFIX;
import static org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam.MASTER_NODE_SELECTOR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_MEMORY_MB;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import junit.framework.TestCase;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowHeartbeatException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KubernetesClusterManagerTest {

    private Configuration jobConf;

    private static final int MASTER_MEMORY = 128;
    private static final String MASTER_COMPONENT = "master";
    private static final String CLUSTER_ID = "geaflow-cluster-1";
    private static final String SERVICE_ACCOUNT = "geaflow";
    private static final String CONF_DIR_IN_IMAGE = "/geaflow/conf";
    private static final String MASTER_CONTAINER_NAME = "geaflow-master";

    private KubernetesClient kubernetesClient;
    private GeaflowKubeClient geaflowKubeClient;
    private KubernetesServer server = new KubernetesServer(false, true);
    private KubernetesClusterManager kubernetesClusterManager;

    @BeforeMethod
    public void setUp() {
        jobConf = new Configuration();
        jobConf.put(ExecutionConfigKeys.RUN_LOCAL_MODE, Boolean.TRUE.toString());
        jobConf.put(KubernetesConfigKeys.CONF_DIR.getKey(), CONF_DIR_IN_IMAGE);
        jobConf.put(ExecutionConfigKeys.CLUSTER_ID.getKey(), CLUSTER_ID);
        jobConf.put(KubernetesMasterParam.MASTER_CONTAINER_NAME, MASTER_CONTAINER_NAME);
        jobConf.put(MASTER_MEMORY_MB.getKey(), String.valueOf(MASTER_MEMORY));
        jobConf.put(MATCH_EXPRESSION_LIST, "key1:In:value1,key2:In:-");
        jobConf.setMasterId(CLUSTER_ID + "_MASTER");

        server.before();
        kubernetesClient = server.getClient();
        geaflowKubeClient = new GeaflowKubeClient(kubernetesClient, jobConf);

        kubernetesClusterManager = new KubernetesClusterManager();
        ClusterContext context = new ClusterContext(jobConf);
        kubernetesClusterManager.init(context, geaflowKubeClient);
    }

    @AfterMethod
    public void destroy() {
        server.after();
    }

    @Test
    public void testCreateMasterContainer() {
        // Set environment for master
        String envName = "env-a";
        String envValue = "value-a";
        jobConf.put(MASTER_ENV_PREFIX + envName, envValue);

        Container container = kubernetesClusterManager
            .createMasterContainer(DockerNetworkType.BRIDGE);

        assertTrue(container.getCommand().isEmpty());
        Optional<EnvVar> commandEnv = container.getEnv().stream()
            .filter(e -> e.getName().equals(K8SConstants.ENV_START_COMMAND)).findAny();
        assertNotNull(commandEnv.get());
        assertNotNull(commandEnv.get().getValue());
        assertNotNull(container.getArgs());
        assertEquals(MASTER_CONTAINER_NAME, container.getName());
        ContainerPort httpPort = new ContainerPortBuilder().withContainerPort(8090)
            .withName(K8SConstants.HTTP_PORT).withProtocol("TCP").build();
        assertTrue(container.getPorts().contains(httpPort));
        assertEquals(MASTER_MEMORY << 20,
            Integer.parseInt(container.getResources().getLimits().get("memory").getAmount()));

        // Check environment
        assertTrue(container.getEnv()
            .contains(new EnvVarBuilder().withName(envName).withValue(envValue).build()));
    }

    @Test
    public void testCreateMaster() {
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        // check config map
        ConfigMap configMap = kubernetesClient.configMaps()
            .withName(CLUSTER_ID + K8SConstants.MASTER_CONFIG_MAP_SUFFIX).get();
        assertNotNull(configMap);
        assertEquals(1, configMap.getData().size());
        assertTrue(configMap.getData().containsKey(K8SConstants.ENV_CONFIG_FILE));

        // check replication controller
        Deployment deployment = geaflowKubeClient.getDeployment(CLUSTER_ID + MASTER_RS_NAME_SUFFIX);
        assertNotNull(deployment);
        assertEquals(1, deployment.getSpec().getReplicas().intValue());
        assertEquals(3, deployment.getSpec().getSelector().getMatchLabels().size());
        assertEquals(MASTER_CONTAINER_NAME,
            deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        TestCase.assertEquals(K8SConstants.GEAFLOW_CONF_VOLUME,
            deployment.getSpec().getTemplate().getSpec().getVolumes().get(0).getName());
        assertEquals(SERVICE_ACCOUNT,
            deployment.getSpec().getTemplate().getSpec().getServiceAccountName());

        // check service
        Service service = geaflowKubeClient.getService(CLUSTER_ID + SERVICE_NAME_SUFFIX);
        assertNotNull(service);
        assertEquals(labels, service.getSpec().getSelector());
        assertEquals(1, service.getSpec().getPorts().size());

        // check owner reference
        String serviceName = service.getMetadata().getName();
        assertNotNull(configMap.getMetadata().getOwnerReferences());
        assertEquals(serviceName, configMap.getMetadata().getOwnerReferences().get(0).getName());
        assertNotNull(deployment.getMetadata().getOwnerReferences().get(0));
        assertEquals(serviceName, deployment.getMetadata().getOwnerReferences().get(0).getName());
    }

    @Test(timeOut = 30000)
    public void testKillCluster() {
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        Service service = geaflowKubeClient.getService(CLUSTER_ID + SERVICE_NAME_SUFFIX);
        assertNotNull(service);

        ConfigMap configMap = geaflowKubeClient
            .getConfigMap(CLUSTER_ID + K8SConstants.MASTER_CONFIG_MAP_SUFFIX);
        assertNotNull(configMap);

        Deployment rc = geaflowKubeClient.getDeployment(CLUSTER_ID + MASTER_RS_NAME_SUFFIX);
        assertNotNull(rc);

        kubernetesClusterManager.close();
    }

    @Test(timeOut = 30000)
    public void testMasterUserLabels() {
        jobConf.put(POD_USER_LABELS.getKey(), "l1:test,l2:hello");
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        labels.put(LABEL_COMPONENT_ID_KEY, String.valueOf(DEFAULT_MASTER_ID));
        labels.putAll(KubernetesUtils.getPairsConf(jobConf, POD_USER_LABELS));
        assertEquals(5, labels.size());
        assertEquals("test", labels.get("l1"));
        assertEquals("hello", labels.get("l2"));
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        // check replication controller
        Deployment rc = kubernetesClient.apps().deployments()
            .withName(CLUSTER_ID + MASTER_RS_NAME_SUFFIX).get();
        assertNotNull(rc);
        assertEquals(labels, rc.getSpec().getSelector().getMatchLabels());

        // check service
        Service service = kubernetesClient.services().withName(CLUSTER_ID + SERVICE_NAME_SUFFIX)
            .get();
        assertNotNull(service);
        assertEquals(labels, service.getSpec().getSelector());
    }

    @Test(timeOut = 30000)
    public void testStartMasterWithNodePort() {
        jobConf.put(SERVICE_EXPOSED_TYPE.getKey(), "NODE_PORT");
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        // check service
        Service service = geaflowKubeClient.getService(CLUSTER_ID + SERVICE_NAME_SUFFIX);
        assertNotNull(service);
        assertEquals(1, service.getSpec().getPorts().size());
        // Check client service
        Service clientService = geaflowKubeClient
            .getService(KubernetesUtils.getMasterClientServiceName(CLUSTER_ID));
        assertNotNull(service);
        assertEquals(1, clientService.getSpec().getPorts().size());
    }

    @Test(timeOut = 30000)
    public void testStartMasterWithNodeSelector() {
        jobConf.put(MASTER_NODE_SELECTOR, "env:production,tier:frontend");
        kubernetesClusterManager.createMaster(CLUSTER_ID, new HashMap<>());

        // check node selector
        Deployment rc = kubernetesClient.apps().deployments()
            .withName(CLUSTER_ID + MASTER_RS_NAME_SUFFIX).get();
        assertNotNull(rc);
        Map<String, String> nodeSelector = rc.getSpec().getTemplate().getSpec().getNodeSelector();
        assertEquals(2, nodeSelector.size());
        assertEquals("production", nodeSelector.get("env"));
        assertEquals("frontend", nodeSelector.get("tier"));
    }

    @Test
    public void testHostNetwork() {
        jobConf
            .put(DOCKER_NETWORK_TYPE.getKey(), KubernetesConfig.DockerNetworkType.HOST.toString());
        kubernetesClusterManager.createMaster(CLUSTER_ID, new HashMap<>());

        // check replication controller
        Deployment rc = geaflowKubeClient.getDeployment(CLUSTER_ID + MASTER_RS_NAME_SUFFIX);
        assertNotNull(rc);
        assertEquals(1, rc.getSpec().getReplicas().intValue());
        assertTrue(rc.getSpec().getTemplate().getSpec().getHostNetwork());
    }

    @Test
    public void testCreateWorkerPod() {
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        int containerId = 1;
        KubernetesClusterManager kubernetesClusterManager2 = new KubernetesClusterManager();
        jobConf.put(CLUSTER_START_TIME, String.valueOf(System.currentTimeMillis()));
        ClusterContext context = new ClusterContext(jobConf);
        kubernetesClusterManager2.init(context, geaflowKubeClient);
        kubernetesClusterManager2.createNewContainer(containerId, false);
        // check pod label
        verifyWorkerPodSize(1);
        // restart pod
        kubernetesClusterManager2.restartContainer(containerId);
        verifyWorkerPodSize(1);
    }

    @Test
    public void testClusterFailover() {

        int masterId = 0;
        int containerId_1 = 1;
        int containerId_2 = 2;
        int driverId = 3;
        Map<Integer, String> containerIds = new HashMap<Integer, String>() {{
            put(containerId_1, "container1");
            put(containerId_2, "container2");
        }};
        Map<Integer, String> driverIds = new HashMap<Integer, String>() {{
            put(0, "driver1");
        }};

        // driver/container FO
        Map<String, String> labels = new HashMap<>();
        labels.put("app", CLUSTER_ID);
        labels.put("component", MASTER_COMPONENT);
        kubernetesClusterManager.createMaster(CLUSTER_ID, labels);

        KubernetesClusterManager kubernetesClusterManager2 = new KubernetesClusterManager();
        jobConf.put(CLUSTER_START_TIME, String.valueOf(System.currentTimeMillis()));
        ClusterContext context = new ClusterContext(jobConf);
        context.setContainerIds(containerIds);
        context.setDriverIds(driverIds);
        kubernetesClusterManager2.init(context, geaflowKubeClient);
        kubernetesClusterManager2.createNewDriver(driverId, 0);
        kubernetesClusterManager2.createNewContainer(containerId_1, false);
        kubernetesClusterManager2.createNewContainer(containerId_2, false);

        // check pod label
        verifyWorkerPodSize(2);

        // restart all pod
        kubernetesClusterManager2.restartContainer(containerId_1);
        verifyWorkerPodSize(2);

        // restart driver & containers
        KubernetesClusterManager mock = mockClusterManager();
        mock.doFailover(masterId, new GeaflowHeartbeatException());
        Mockito.verify(mock, Mockito.times(1)).restartAllDrivers();
        Mockito.verify(mock, Mockito.times(1)).restartAllContainers();
    }

    private KubernetesClusterManager mockClusterManager() {
        KubernetesClusterManager clusterManager = new KubernetesClusterManager();
        KubernetesClusterManager mockClusterManager = Mockito.spy(clusterManager);
        Mockito.doNothing().when(mockClusterManager).restartAllDrivers();
        Mockito.doNothing().when(mockClusterManager).restartAllContainers();

        ClusterContext context = new ClusterContext(jobConf);
        mockClusterManager.init(context, geaflowKubeClient);
        return mockClusterManager;
    }

    private void verifyWorkerPodSize(int size) {
        verifyPodSize(size, K8SConstants.LABEL_COMPONENT_WORKER);
    }

    private void verifyPodSize(int size, String componentKey) {
        List<Pod> pods = getPods(componentKey);
        Assert.assertEquals(pods.size(), size);
        if (size > 0) {
            Assert.assertTrue(pods.get(0).getMetadata().getLabels().containsKey(K8SConstants.LABEL_COMPONENT_ID_KEY));
        }
    }

    private List<Pod> getPods(String componentKey) {
        Map<String, String> workerLabels = new HashMap<>();
        workerLabels.put(K8SConstants.LABEL_APP_KEY, CLUSTER_ID);
        workerLabels.put(K8SConstants.LABEL_COMPONENT_KEY, componentKey);
        return geaflowKubeClient.getPods(workerLabels).getItems();
    }

}
