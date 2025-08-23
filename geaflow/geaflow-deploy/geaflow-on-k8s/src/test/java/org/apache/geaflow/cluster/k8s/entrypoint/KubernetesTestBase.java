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

package org.apache.geaflow.cluster.k8s.entrypoint;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.HashMap;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesResourceBuilder;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class KubernetesTestBase {

    private final KubernetesServer server = new KubernetesServer(false, true);
    protected KubernetesClient kubernetesClient;
    protected String masterUrl;
    protected String clusterId;
    protected Configuration configuration;
    protected GeaflowKubeClient geaflowKubeClient;

    @BeforeMethod
    public void setUp() {
        server.before();
        kubernetesClient = server.getClient();
        masterUrl = kubernetesClient.getMasterUrl().toString();
        clusterId = "123";

        configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "memory");
        configuration.put(ExecutionConfigKeys.REPORTER_LIST, "slf4j");
        configuration.put(ExecutionConfigKeys.HA_SERVICE_TYPE, "memory");
        configuration.put(ExecutionConfigKeys.JOB_UNIQUE_ID, "1");
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, clusterId);
        configuration.put(KubernetesConfigKeys.MASTER_URL, masterUrl);
        configuration.put(KubernetesConfig.CLUSTER_START_TIME,
            String.valueOf(System.currentTimeMillis()));
        configuration.setMasterId("mockMaster");

        geaflowKubeClient = new GeaflowKubeClient(configuration, masterUrl);
        createService(configuration);
    }

    @AfterMethod
    public void destroy() {
        server.after();
    }

    private void createService(Configuration configuration) {
        KubernetesMasterParam param = new KubernetesMasterParam(configuration);
        String serviceName = KubernetesUtils.getMasterServiceName(clusterId);
        Service service = KubernetesResourceBuilder
            .createService(serviceName, ServiceExposedType.CLUSTER_IP,
                new HashMap<>(), null, param);
        geaflowKubeClient.createService(service);
    }
}
