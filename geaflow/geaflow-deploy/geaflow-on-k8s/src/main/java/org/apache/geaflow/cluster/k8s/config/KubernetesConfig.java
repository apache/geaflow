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

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONNECTION_RETRY_INTERVAL_MS;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONNECTION_RETRY_TIMES;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DEFAULT_RESOURCE_EPHEMERAL_STORAGE_SIZE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DOCKER_NETWORK_TYPE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.ENABLE_RESOURCE_CPU_LIMIT;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.ENABLE_RESOURCE_EPHEMERAL_STORAGE_LIMIT;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.ENABLE_RESOURCE_MEMORY_LIMIT;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.MASTER_URL;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.WORK_DIR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_CLIENT_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesConfig.class);

    public static final String CLUSTER_START_TIME = "kubernetes.cluster.start-time";

    public static final String CLIENT_MASTER_URL = "kubernetes.client.master.url";

    public static final String MASTER_EXPOSED_ADDRESS = "kubernetes.master.exposed.address";

    public static final String DRIVER_EXPOSED_ADDRESS = "kubernetes.driver.exposed.address";

    /**
     * Service exposed type on kubernetes cluster.
     */
    public enum ServiceExposedType {
        CLUSTER_IP("ClusterIP"),
        NODE_PORT("NodePort"),
        LOAD_BALANCER("LoadBalancer"),
        EXTERNAL_NAME("ExternalName");

        private final String serviceExposedType;

        ServiceExposedType(String type) {
            serviceExposedType = type;
        }

        public String getServiceExposedType() {
            return serviceExposedType;
        }

        /**
         * Convert exposed type string in kubernetes spec to ServiceExposedType.
         *
         * @param type exposed in kubernetes spec, e.g. LoadBanlancer
         * @return ServiceExposedType
         */
        public static ServiceExposedType fromString(String type) {
            for (ServiceExposedType exposedType : ServiceExposedType.values()) {
                if (exposedType.getServiceExposedType().equals(type)) {
                    return exposedType;
                }
            }
            return CLUSTER_IP;
        }
    }

    /**
     * The network type which be used by docker daemon.
     */
    public enum DockerNetworkType {
        BRIDGE,
        HOST;

        /**
         * Convert network type string to DockerNetworkType.
         *
         * @param type Docker network type, e.g. bridge
         * @return DockerNetworkType
         */
        public static DockerNetworkType fromString(String type) {
            for (DockerNetworkType dockerNetworkType : DockerNetworkType.values()) {
                if (dockerNetworkType.toString().equals(type)) {
                    return dockerNetworkType;
                }
            }
            LOGGER.warn("Docker network type {} is not supported, BRIDGE network will be used", type);
            return BRIDGE;
        }
    }

    public static String getClientMasterUrl(Configuration config) {
        String url = config.getString(CLIENT_MASTER_URL);
        if (StringUtils.isNotBlank(url)) {
            return url;
        } else {
            return config.getString(MASTER_URL);
        }
    }

    public static int getClientTimeoutMs(Configuration config) {
        return config.getInteger(CLUSTER_CLIENT_TIMEOUT_MS);
    }

    public static int getConnectionRetryTimes(Configuration config) {
        return config.getInteger(CONNECTION_RETRY_TIMES);
    }

    public static long getConnectionRetryIntervalMs(Configuration config) {
        return config.getLong(CONNECTION_RETRY_INTERVAL_MS);
    }

    public static DockerNetworkType getDockerNetworkType(Configuration config) {
        String value = config.getString(DOCKER_NETWORK_TYPE);
        return DockerNetworkType.fromString(value);
    }

    public static ServiceExposedType getServiceExposedType(Configuration config) {
        String value = config.getString(SERVICE_EXPOSED_TYPE);
        return ServiceExposedType.valueOf(value);
    }

    public static String getServiceName(Configuration config) {
        return KubernetesUtils.getMasterServiceName(config.getString(CLUSTER_ID));
    }

    public static String getServiceNameWithNamespace(Configuration config) {
        return getServiceName(config) + K8SConstants.NAMESPACE_SEPARATOR + config.getString(NAME_SPACE);
    }

    public static boolean enableResourceMemoryLimit(Configuration config) {
        return config.getBoolean(ENABLE_RESOURCE_MEMORY_LIMIT);
    }

    public static boolean enableResourceCpuLimit(Configuration config) {
        return config.getBoolean(ENABLE_RESOURCE_CPU_LIMIT);
    }

    public static boolean enableResourceEphemeralStorageLimit(Configuration config) {
        return config.getBoolean(ENABLE_RESOURCE_EPHEMERAL_STORAGE_LIMIT);
    }

    public static String getResourceEphemeralStorageSize(Configuration config) {
        return config.getString(DEFAULT_RESOURCE_EPHEMERAL_STORAGE_SIZE);
    }

    public static String getJarDownloadPath(Configuration config) {
        return FileUtils.getFile(config.getString(WORK_DIR), "jar").toString();
    }

}
