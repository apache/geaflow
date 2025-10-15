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

/**
 * Constants for kubernetes.
 */
public final class K8SConstants {

    public static final String RANDOM_CLUSTER_ID_PREFIX = "geaflow";

    public static final String RPC_PORT = "rpc";

    public static final String HTTP_PORT = "rest";

    public static final String NAME_SEPARATOR = "-";

    public static final String NAMESPACE_SEPARATOR = ".";

    public static final String CONFIG_KV_SEPARATOR = ":";

    public static final String ADDRESS_SEPARATOR = "=";

    public static final String CONFIG_LIST_SEPARATOR = ",";

    public static final String MASTER_RS_NAME_SUFFIX = "-master-rs";

    public static final String DRIVER_RS_NAME_SUFFIX = "-driver-rs-";

    public static final String CLIENT_NAME_SUFFIX = "-client";

    public static final String MASTER_NAME_SUFFIX = "-master";

    public static final String DRIVER_NAME_SUFFIX = "-driver";

    public static final String WORKER_NAME_SUFFIX = "-container";

    public static final String SERVICE_NAME_SUFFIX = "-service";

    public static final String CLIENT_SERVICE_NAME_SUFFIX = "-client-service";

    public static final String DRIVER_SERVICE_NAME_SUFFIX = "-driver-service-";

    public static final String MASTER_CONFIG_MAP_SUFFIX = "-master-conf-map";

    public static final String WORKER_CONFIG_MAP_SUFFIX = "-worker-conf-map";

    public static final String CLIENT_CONFIG_MAP_SUFFIX = "-client-conf-map";

    public static final String LABEL_APP_KEY = "app";

    public static final String LABEL_CONFIG_MAP_LOCK = "config-map-lock";

    public static final String LABEL_COMPONENT_KEY = "component";

    public static final String LABEL_COMPONENT_MASTER = "master";

    public static final String LABEL_COMPONENT_WORKER = "worker";

    public static final String LABEL_COMPONENT_DRIVER = "driver";

    public static final String LABEL_COMPONENT_CLIENT = "client";

    public static final String LABEL_COMPONENT_ID_KEY = "component-id";

    public static final String RESOURCE_NAME_MEMORY = "memory";

    public static final String RESOURCE_NAME_CPU = "cpu";

    public static final String RESOURCE_NAME_EPHEMERAL_STORAGE = "ephemeral-storage";

    public static final String POD_RESTART_POLICY = "Always";

    public static final String HOST_ALIASES_CONFIG_MAP_NAME = "host-aliases";

    public static final String HOST_NETWORK_DNS_POLICY = "ClusterFirstWithHostNet";

    public static final String TCP_PROTOCOL = "TCP";

    public static final String ENV_NODE_NAME = "NODE_NAME";
    public static final String NODE_NAME_FIELD_PATH = "spec.nodeName";

    public static final String ENV_POD_NAME = "POD_NAME";
    public static final String POD_NAME_FIELD_PATH = "metadata.name";

    public static final String ENV_POD_IP = "POD_IP";
    public static final String POD_IP_FIELD_PATH = "status.podIP";

    public static final String ENV_HOST_IP = "HOST_IP";
    public static final String HOST_IP_FIELD_PATH = "status.hostIP";

    public static final String ENV_SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
    public static final String SERVICE_ACCOUNT_NAME_FIELD_PATH = "spec.serviceAccountName";

    public static final String ENV_NAMESPACE = "NAMESPACE";
    public static final String NAMESPACE_FIELD_PATH = "metadata.namespace";

    // ----------------------------- Environment Variables ----------------------------

    public static final String ENV_CONF_DIR = "GEAFLOW_CONF_DIR";

    public static final String ENV_LOG_DIR = "GEAFLOW_LOG_DIR";

    public static final String ENV_JOB_WORK_PATH = "GEAFLOW_JOB_WORK_PATH";

    public static final String ENV_JAR_DOWNLOAD_PATH = "GEAFLOW_JAR_DOWNLOAD_PATH";

    public static final String ENV_UDF_LIST = "GEAFLOW_UDF_LIST";

    public static final String ENV_ENGINE_JAR = "GEAFLOW_ENGINE_JAR";

    public static final String ENV_PERSISTENT_ROOT = "GEAFLOW_PERSISTENT_ROOT";

    public static final String ENV_CATALOG_TOKEN = "GEAFLOW_CATALOG_TOKEN";

    public static final String ENV_GW_ENDPOINT = "GEAFLOW_GW_ENDPOINT";

    public static final String ENV_START_COMMAND = "GEAFLOW_START_COMMAND";

    public static final String ENV_CONTAINER_ID = "GEAFLOW_CONTAINER_ID";

    public static final String ENV_CONTAINER_INDEX = "GEAFLOW_CONTAINER_INDEX";

    public static final String ENV_IS_RECOVER = "GEAFLOW_IS_RECOVER";

    public static final String ENV_AUTO_RESTART = "GEAFLOW_AUTO_RESTART";

    public static final String ENV_ALWAYS_DOWNLOAD_ENGINE = "GEAFLOW_ALWAYS_DOWNLOAD_ENGINE_JAR";

    public static final String ENV_CLUSTER_ID = "GEAFLOW_CLUSTER_ID";

    public static final String ENV_CLUSTER_FAULT_INJECTION_ENABLE =
        "GEAFLOW_CLUSTER_FAULT_INJECTION_ENABLE";

    public static final String ENV_MASTER_ID = "GEAFLOW_MASTER_ID";

    public static final String ENV_CONFIG_FILE = "geaflow-conf.yml";
    public static final String ENV_PROFILER_PATH = "ASYNC_PROFILER_SHELL_PATH";

    public static final String GEAFLOW_CONF_VOLUME = "geaflow-conf-volume";

    public static final String GEAFLOW_LOG_VOLUME = "geaflow-log-volume";

    public static final String MASTER_ADDRESS = "geaflow.master.address";

    public static final String JOB_CLASSPATH = "$GEAFLOW_CLASSPATH";

}
