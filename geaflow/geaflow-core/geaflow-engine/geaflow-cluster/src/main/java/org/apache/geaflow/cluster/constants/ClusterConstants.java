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

public class ClusterConstants {

    private static final String MASTER_PREFIX = "master-";
    private static final String DRIVER_PREFIX = "driver-";
    private static final String CONTAINER_PREFIX = "container-";

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

    public static String getMasterName() {
        return String.format("%s%s", MASTER_PREFIX, DEFAULT_MASTER_ID);
    }

    public static String getDriverName(int id) {
        return String.format("%s%s", DRIVER_PREFIX, id);
    }

    public static String getContainerName(int id) {
        return String.format("%s%s", CONTAINER_PREFIX, id);
    }

}
