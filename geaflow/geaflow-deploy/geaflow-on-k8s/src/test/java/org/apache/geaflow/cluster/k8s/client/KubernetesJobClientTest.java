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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.CLIENT_NAME_SUFFIX;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_DISK_GB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_MEMORY_MB;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.List;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.utils.SleepUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KubernetesJobClientTest {

    private Configuration jobConf;

    private static final int MASTER_MEMORY = 128;
    private static final String CLUSTER_ID = "geaflow-cluster-1";
    private static final String CONF_DIR_IN_IMAGE = "/geaflow/conf";
    private static final String MASTER_CONTAINER_NAME = "geaflow-master";

    private final KubernetesServer server = new KubernetesServer(false, true);
    private KubernetesJobClient jobClient;
    private KubernetesClient kubernetesClient;
    private GeaflowKubeClient geaflowKubeClient;

    @BeforeMethod
    public void setUp() {
        jobConf = new Configuration();
        jobConf.put(KubernetesConfigKeys.CONF_DIR.getKey(), CONF_DIR_IN_IMAGE);
        jobConf.put(ExecutionConfigKeys.CLUSTER_ID.getKey(), CLUSTER_ID);
        jobConf.put(KubernetesMasterParam.MASTER_CONTAINER_NAME, MASTER_CONTAINER_NAME);
        jobConf.put(MASTER_MEMORY_MB.getKey(), String.valueOf(MASTER_MEMORY));
        jobConf.setMasterId(CLUSTER_ID + "_MASTER");
        jobConf.put(CLIENT_DISK_GB.getKey(), "5");

        server.before();
        kubernetesClient = server.getClient();
        geaflowKubeClient = new GeaflowKubeClient(kubernetesClient, jobConf);
        jobClient = new KubernetesJobClient(jobConf.getConfigMap(), geaflowKubeClient);
    }

    @AfterMethod
    public void destroy() {
        jobClient.stopJob();
        server.after();
    }

    @Test
    public void testCreateJobClient() {
        jobClient.submitJob();
        SleepUtils.sleepSecond(2);

        // check config map
        String configMapName = CLUSTER_ID + K8SConstants.CLIENT_CONFIG_MAP_SUFFIX;
        ConfigMap configMap = kubernetesClient.configMaps().withName(configMapName).get();
        assertNotNull(configMap);
        assertEquals(1, configMap.getData().size());
        assertTrue(configMap.getData().containsKey(K8SConstants.ENV_CONFIG_FILE));

        // check pod reference
        Pod pod =
            kubernetesClient.pods().withName(CLUSTER_ID + CLIENT_NAME_SUFFIX).get();
        assertNotNull(configMap.getMetadata().getOwnerReferences());
        assertEquals(configMapName, pod.getMetadata().getOwnerReferences().get(0).getName());
        assertEquals(pod.getSpec().getContainers().size(), 1);

        List<Pod> jobPods = jobClient.getJobPods();
        Pod jobPod = jobPods.get(0);
        assertEquals(jobPod.getSpec().getContainers().size(), 1);
    }
}
