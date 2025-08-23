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

package org.apache.geaflow.cluster.ray.config;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class RayConfig {

    public static final String RAY_RUN_MODE = "ray.run-mode";

    public static final String RAY_JOB_JVM_OPTIONS_PREFIX = "ray.job.jvm-options";
    public static final String RAY_TASK_RETURN_TASK_EXCEPTION = "ray.task.return_task_exception";
    public static final String RAY_JOB_L1FO_ENABLE = "ray.job.enable-l1-fault-tolerance";

    public static final String RAY_JOB_RUNTIME_ENV = "ray.job.runtime-env";
    public static final String RAY_JOB_WORKING_DIR = "working_dir";

    public static final int CLUSTER_RESERVED_MEMORY_MB = 3 * 1024;
    public static final int WORKER_RESERVED_MEMORY_MB = 3 * 1024;


    // Sets the amount of memory occupied by a jvm process, both in and out of the heap
    public static final String RAY_JOB_JAVA_WORKER_PROCESS_DEFAULT_MEMORY_MB =
        "ray.job.java-worker-process-default-memory-mb";

    // The proportion of Xmx to memory_mb
    public static final String RAY_JOB_JAVA_HEAP_FRACTION = "ray.job.java-heap-fraction";

    // Set the initialization to start how many jvm processes there
    public static final String RAY_JOB_NUM_INITIAL_JAVA_WORKER_PROCESS =
        "ray.job.num-initial-java-worker-processes";

    // Sets the total memory for initializing startup jobs
    public static final String RAY_JOB_TOTAL_MEMORY_MB = "ray.job.total-memory-mb";

    // User - defined log file
    public static final String RAY_CUSTOM_LOGGER0_NAME = "ray.logging.loggers.0.name";

    public static final String RAY_CUSTOM_LOGGER0_FILE_NAME = "ray.logging.loggers.0.file-name";

    public static final String RAY_CUSTOM_LOGGER0_PATTERN = "ray.logging.loggers.0.pattern";

    public static final String CUSTOM_LOGGER_NAME = "userlogger";

    public static final String CUSTOM_LOGGER_FILE_NAME = "geaflow-user-%p.log";

    public static final String CUSTOM_LOGGER_PATTERN = "%d{yyyy-MM-dd HH:mm:ss,SSS} %p %c{1} [%t]: %m%n";

    public static final String RAY_LOG_DIR = "/home/admin/logs/ray-logs/logs";

    public static final ConfigKey RAY_AGENT_PROFILER_PATH = ConfigKeys
        .key("ray.agent.profiler.path")
        .defaultValue("/home/admin/ray-pack/bin/profiler/profiler.sh")
        .description("ray agent profiler path");

}

