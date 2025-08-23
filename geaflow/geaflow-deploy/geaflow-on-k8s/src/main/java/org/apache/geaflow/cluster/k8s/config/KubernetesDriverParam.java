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

import static org.apache.geaflow.cluster.constants.ClusterConstants.DRIVER_LOG_SUFFIX;
import static org.apache.geaflow.cluster.k8s.config.K8SConstants.JOB_CLASSPATH;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DRIVER_NODE_PORT;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_RPC_PORT;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.config.ClusterConfig;
import org.apache.geaflow.cluster.k8s.entrypoint.KubernetesDriverRunner;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;

public class KubernetesDriverParam extends AbstractKubernetesParam {

    public static final String DRIVER_ENV_PREFIX = "kubernetes.driver.env.";

    public static final String DRIVER_USER_ANNOTATIONS = "kubernetes.driver.user.annotations";

    public static final String DRIVER_NODE_SELECTOR = "kubernetes.driver.node-selector";

    public KubernetesDriverParam(Configuration config) {
        super(config);
    }

    public KubernetesDriverParam(ClusterConfig config) {
        super(config);
    }

    @Override
    public Double getContainerCpu() {
        return clusterConfig.getDriverVcores();
    }

    @Override
    public long getContainerMemoryMB() {
        return clusterConfig.getDriverMemoryMB();
    }

    @Override
    protected long getContainerDiskGB() {
        return clusterConfig.getDriverDiskGB();
    }

    @Override
    public String getContainerShellCommand() {
        String logFileName = getLogDir() + File.separator + DRIVER_LOG_SUFFIX;
        return ClusterUtils.getStartCommand(clusterConfig.getDriverJvmOptions(),
            KubernetesDriverRunner.class, logFileName, config, JOB_CLASSPATH);
    }

    @Override
    public Map<String, String> getAdditionEnvs() {
        return KubernetesUtils
            .getVariablesWithPrefix(DRIVER_ENV_PREFIX, config.getConfigMap());
    }

    @Override
    public String getPodNamePrefix(String clusterId) {
        return clusterId + K8SConstants.DRIVER_NAME_SUFFIX + K8SConstants.NAME_SEPARATOR;
    }

    @Override
    public String getConfigMapName(String clusterId) {
        return clusterId + K8SConstants.WORKER_CONFIG_MAP_SUFFIX;
    }

    @Override
    public int getRpcPort() {
        return config.getInteger(DRIVER_RPC_PORT);
    }

    public int getNodePort() {
        return config.getInteger(DRIVER_NODE_PORT);
    }

    @Override
    public Map<String, String> getPodLabels(String clusterId) {
        Map<String, String> driverPodLabels = new HashMap<>();
        driverPodLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        driverPodLabels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_DRIVER);
        driverPodLabels.putAll(KubernetesUtils.getPairsConf(config, POD_USER_LABELS));
        return driverPodLabels;
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return KubernetesUtils.getPairsConf(config, DRIVER_NODE_SELECTOR);
    }

    @Override
    public Map<String, String> getAnnotations() {
        return KubernetesUtils.getPairsConf(config, DRIVER_USER_ANNOTATIONS);
    }

}
