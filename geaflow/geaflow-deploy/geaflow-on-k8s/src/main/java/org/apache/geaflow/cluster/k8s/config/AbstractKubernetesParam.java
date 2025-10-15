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

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CLUSTER_FAULT_INJECTION_ENABLE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONF_DIR;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_IMAGE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_IMAGE_PULL_POLICY;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.LOG_DIR;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_ACCOUNT;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_USER_ANNOTATIONS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_USER_LABELS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
import java.util.Map;
import org.apache.geaflow.cluster.config.ClusterConfig;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;

public abstract class AbstractKubernetesParam implements KubernetesParam {

    protected Configuration config;
    protected ClusterConfig clusterConfig;

    public AbstractKubernetesParam(Configuration config) {
        this.config = config;
        this.clusterConfig = ClusterConfig.build(config);
    }

    public AbstractKubernetesParam(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.config = clusterConfig.getConfig();
    }

    public String getContainerImage() {
        return config.getString(CONTAINER_IMAGE);
    }

    public String getContainerImagePullPolicy() {
        return config.getString(CONTAINER_IMAGE_PULL_POLICY);
    }

    public String getServiceAccount() {
        return config.getString(SERVICE_ACCOUNT);
    }

    public Map<String, String> getServiceLabels() {
        return KubernetesUtils.getPairsConf(config, SERVICE_USER_LABELS);
    }

    @Override
    public Map<String, String> getServiceAnnotations() {
        return KubernetesUtils.getPairsConf(config, SERVICE_USER_ANNOTATIONS.getKey());
    }

    protected Quantity getCpuQuantity(double cpu) {
        return new QuantityBuilder(false).withAmount(String.valueOf(cpu)).build();
    }

    protected Quantity getMemoryQuantity(long memoryMB) {
        return new QuantityBuilder(false).withAmount(String.valueOf((memoryMB) << 20))
            .build();
    }

    protected Quantity getDiskQuantity(long diskGB) {
        return new QuantityBuilder(false).withAmount(String.valueOf((diskGB) << 30))
            .build();
    }

    @Override
    public String getConfDir() {
        return config.getString(CONF_DIR);
    }

    @Override
    public String getLogDir() {
        return config.getString(LOG_DIR);
    }

    @Override
    public String getAutoRestart() {
        return config.getString(PROCESS_AUTO_RESTART);
    }

    @Override
    public Boolean getClusterFaultInjectionEnable() {
        return config.getBoolean(CLUSTER_FAULT_INJECTION_ENABLE);
    }

    @Override
    public int getRpcPort() {
        return 0;
    }

    @Override
    public int getHttpPort() {
        return 0;
    }

    public int getNodePort() {
        return 0;
    }

    @Override
    public Quantity getCpuQuantity() {
        Double cpu = getContainerCpu();
        return getCpuQuantity(cpu);
    }

    protected abstract Double getContainerCpu();

    @Override
    public Quantity getMemoryQuantity() {
        long memoryMB = getContainerMemoryMB();
        return getMemoryQuantity(memoryMB);
    }

    protected abstract long getContainerMemoryMB();

    @Override
    public Quantity getDiskQuantity() {
        long diskGB = getContainerDiskGB();
        if (diskGB == 0) {
            return null;
        }
        return getDiskQuantity(diskGB);
    }

    protected abstract long getContainerDiskGB();

    @Override
    public Map<String, String> getAdditionEnvs() {
        return null;
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    @Override
    public boolean enableLeaderElection() {
        return false;
    }
}
