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

package org.apache.geaflow.example.k8s;

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.DEFAULT_RESOURCE_EPHEMERAL_STORAGE_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_JVM_OPTIONS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_MEMORY_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_VCORES;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_JVM_OPTION;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_MEMORY_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_JVM_OPTION;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_MEMORY_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_JVM_OPTIONS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_MEMORY_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_VCORES;
import static org.apache.geaflow.file.FileConfigKeys.ROOT;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.store.redis.RedisConfigKeys;

public class KubernetesTestBase {

    protected static final String APP_ID = "k8s-cluster-1234";

    protected static final String CONTAINER_IMAGE = "geaflow-arm:0.1";

    protected static final String MASTER_URL = "http://127.0.0.1:54448/";

    protected static final String REDIS_HOST = "host.minikube.internal";

    protected Map<String, String> config;

    protected String localConfDir;

    public void setup() {
        config = new HashMap<>();
        config.put(KubernetesConfig.CLIENT_MASTER_URL, MASTER_URL);
        config.put(ExecutionConfigKeys.CLUSTER_ID.getKey(), APP_ID);
        config.put(ExecutionConfigKeys.JOB_UNIQUE_ID.getKey(), APP_ID);
        config.put(KubernetesConfigKeys.CONTAINER_IMAGE.getKey(), CONTAINER_IMAGE);
        config.put(MASTER_VCORES.getKey(), "1");
        config.put(CLIENT_VCORES.getKey(), "1");
        config.put(MASTER_MEMORY_MB.getKey(), "512");
        config.put(MASTER_JVM_OPTIONS.getKey(),
            "-Xmx256m,-Xms256m,-Xmn64m,-XX:MaxDirectMemorySize=158m");
        config.put(CONTAINER_MEMORY_MB.getKey(), "512");
        config.put(CONTAINER_JVM_OPTION.getKey(),
            "-Xmx128m,-Xms128m,-Xmn32m,-XX:MaxDirectMemorySize=128m");
        config.put(DRIVER_MEMORY_MB.getKey(), "512");
        config.put(DRIVER_JVM_OPTION.getKey(),
            "-Xmx128m,-Xms128m,-Xmn32m,-XX:MaxDirectMemorySize=128m");
        config.put(CLIENT_MEMORY_MB.getKey(), "512");
        config.put(CLIENT_JVM_OPTIONS.getKey(),
            "-Xmx256m,-Xms256m,-Xmn64m,-XX:MaxDirectMemorySize=158m");
        config.put(RedisConfigKeys.REDIS_HOST.getKey(), REDIS_HOST);
        config.put(DEFAULT_RESOURCE_EPHEMERAL_STORAGE_SIZE.getKey(), "1Gi");
        config.put(ROOT.getKey(), "/tmp/geaflow/chk");

        localConfDir = this.getClass().getResource("/").getPath();
    }

    public Map<String, String> getConfig() {
        return config;
    }

}
