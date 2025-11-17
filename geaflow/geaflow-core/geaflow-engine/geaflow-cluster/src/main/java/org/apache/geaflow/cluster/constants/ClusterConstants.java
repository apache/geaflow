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

package org.apache.geaflow.cluster.constants;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;

public class ClusterConstants {

    public static final String MASTER_PREFIX = "master-";
    public static final String DRIVER_PREFIX = "driver-";
    public static final String CONTAINER_PREFIX = "container-";

    public static final String MASTER_LOG_SUFFIX = "master.log";
    public static final String DRIVER_LOG_SUFFIX = "driver.log";
    public static final String CONTAINER_LOG_SUFFIX = "container.log";
    public static final String CLUSTER_TYPE = "clusterType";
    public static final String LOCAL_CLUSTER = "LOCAL";

    public static final int DEFAULT_MASTER_ID = 0;
    public static final int EXIT_CODE = -1;

    public static final String ENV_AGENT_PORT = "AGENT_PORT";
    public static final String ENV_SUPERVISOR_PORT = "SUPERVISOR_PORT";

    public static final String MASTER_ID = "GEAFLOW_MASTER_ID";
    public static final String CONTAINER_ID = "GEAFLOW_CONTAINER_ID";
    public static final String CONTAINER_INDEX = "GEAFLOW_CONTAINER_INDEX";
    public static final String AUTO_RESTART = "GEAFLOW_AUTO_RESTART";
    public static final String IS_RECOVER = "GEAFLOW_IS_RECOVER";
    public static final String JOB_CONFIG = "GEAFLOW_JOB_CONFIG";
    public static final String CONTAINER_START_COMMAND = "CONTAINER_START_COMMAND";
    public static final String CONTAINER_START_COMMAND_TEMPLATE =
        "%java% %classpath% %jvmmem% %jvmopts% %logging% %class% %redirects%";
    public static final String AGENT_PROFILER_PATH = "AGENT_PROFILER_PATH";
    public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

    /**
     * Get driver name prefix from configuration or use default value.
     */
    public static String getDriverPrefix(Configuration config) {
        if (config != null) {
            return config.getString(ExecutionConfigKeys.CLUSTER_DRIVER_PREFIX);
        }
        return DRIVER_PREFIX;
    }

    /**
     * Get container name prefix from configuration or use default value.
     */
    public static String getContainerPrefix(Configuration config) {
        if (config != null) {
            return config.getString(ExecutionConfigKeys.CLUSTER_CONTAINER_PREFIX);
        }
        return CONTAINER_PREFIX;
    }

    /**
     * Get master log suffix from configuration or use default value.
     */
    public static String getMasterLogSuffix(Configuration config) {
        if (config != null) {
            return config.getString(ExecutionConfigKeys.CLUSTER_MASTER_LOG_SUFFIX);
        }
        return MASTER_LOG_SUFFIX;
    }

    /**
     * Get driver log suffix from configuration or use default value.
     */
    public static String getDriverLogSuffix(Configuration config) {
        if (config != null) {
            return config.getString(ExecutionConfigKeys.CLUSTER_DRIVER_LOG_SUFFIX);
        }
        return DRIVER_LOG_SUFFIX;
    }

    /**
     * Get container log suffix from configuration or use default value.
     */
    public static String getContainerLogSuffix(Configuration config) {
        if (config != null) {
            return config.getString(ExecutionConfigKeys.CLUSTER_CONTAINER_LOG_SUFFIX);
        }
        return CONTAINER_LOG_SUFFIX;
    }

    /**
     * Get default master id from configuration or use default value.
     */
    public static int getDefaultMasterId(Configuration config) {
        if (config != null) {
            return config.getInteger(ExecutionConfigKeys.CLUSTER_DEFAULT_MASTER_ID);
        }
        return DEFAULT_MASTER_ID;
    }

    /**
     * Get master name using default values (for backward compatibility).
     */
    public static String getMasterName() {
        return String.format("%s%s", MASTER_PREFIX, DEFAULT_MASTER_ID);
    }

    /**
     * Get master name from configuration or use default value.
     * Note: Master prefix is not configurable, always uses the default value.
     */
    public static String getMasterName(Configuration config) {
        return String.format("%s%s", MASTER_PREFIX, getDefaultMasterId(config));
    }

    /**
     * Get driver name using default values (for backward compatibility).
     */
    public static String getDriverName(int id) {
        return String.format("%s%s", DRIVER_PREFIX, id);
    }

    /**
     * Get driver name from configuration or use default value.
     */
    public static String getDriverName(Configuration config, int id) {
        return String.format("%s%s", getDriverPrefix(config), id);
    }

    /**
     * Get container name using default values (for backward compatibility).
     */
    public static String getContainerName(int id) {
        return String.format("%s%s", CONTAINER_PREFIX, id);
    }

    /**
     * Get container name from configuration or use default value.
     */
    public static String getContainerName(Configuration config, int id) {
        return String.format("%s%s", getContainerPrefix(config), id);
    }

}
