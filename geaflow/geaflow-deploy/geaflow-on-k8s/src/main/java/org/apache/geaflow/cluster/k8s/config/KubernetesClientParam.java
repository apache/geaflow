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

package org.apache.geaflow.cluster.k8s.config;

import static org.apache.geaflow.cluster.k8s.config.K8SConstants.JOB_CLASSPATH;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.k8s.entrypoint.KubernetesClientRunner;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;

public class KubernetesClientParam extends AbstractKubernetesParam {

    public static final String CLIENT_USER_ANNOTATIONS = "kubernetes.client.user.annotations";

    public static final String CLIENT_NODE_SELECTOR = "kubernetes.client.node-selector";

    public static final String CONTAINERIZED_CLIENT_ENV_PREFIX = "containerized.client.env.";

    public static final String CLIENT_LOG_SUFFIX = "client.log";

    private static final String CLIENT_AUTO_RESTART = "false";

    public KubernetesClientParam(Configuration config) {
        super(config);
    }

    @Override
    public Double getContainerCpu() {
        return clusterConfig.getClientVcores();
    }

    @Override
    public long getContainerMemoryMB() {
        return clusterConfig.getClientMemoryMB();
    }

    @Override
    protected long getContainerDiskGB() {
        return clusterConfig.getClientDiskGB();
    }

    @Override
    public String getContainerShellCommand() {
        String logFilename = getLogDir() + File.separator + CLIENT_LOG_SUFFIX;
        return ClusterUtils.getStartCommand(clusterConfig.getClientJvmOptions(),
            KubernetesClientRunner.class, logFilename, config, JOB_CLASSPATH);
    }

    @Override
    public Map<String, String> getAdditionEnvs() {
        return KubernetesUtils.getVariablesWithPrefix(CONTAINERIZED_CLIENT_ENV_PREFIX,
            config.getConfigMap());
    }

    @Override
    public String getPodNamePrefix(String clusterId) {
        return clusterId + K8SConstants.CLIENT_NAME_SUFFIX + K8SConstants.NAME_SEPARATOR;
    }

    @Override
    public String getConfigMapName(String clusterId) {
        return clusterId + K8SConstants.CLIENT_CONFIG_MAP_SUFFIX;
    }

    @Override
    public Map<String, String> getPodLabels(String clusterId) {
        Map<String, String> workerPodLabels = new HashMap<>();
        workerPodLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        workerPodLabels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_CLIENT);
        workerPodLabels.putAll(KubernetesUtils.getPairsConf(config, POD_USER_LABELS));
        return workerPodLabels;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return KubernetesUtils.getPairsConf(config, CLIENT_USER_ANNOTATIONS);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return KubernetesUtils.getPairsConf(config, CLIENT_NODE_SELECTOR);
    }

    @Override
    public String getAutoRestart() {
        return CLIENT_AUTO_RESTART;
    }
}
