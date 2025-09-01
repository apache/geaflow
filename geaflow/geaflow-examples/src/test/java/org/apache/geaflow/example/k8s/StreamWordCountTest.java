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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_EXIT_WAIT_SECONDS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_WORKER_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.k8s.client.KubernetesJobClient;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import org.apache.geaflow.metrics.common.reporter.ReporterRegistry;

public class StreamWordCountTest extends KubernetesTestBase {

    public StreamWordCountTest() {
        super.setup();
        config.put(CONTAINER_NUM.getKey(), "1");
        config.put(CONTAINER_WORKER_NUM.getKey(), "2");
        config.put(REPORTER_LIST.getKey(), ReporterRegistry.SLF4J_REPORTER);
        config.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());
        config.put(CLIENT_EXIT_WAIT_SECONDS.getKey(), "120");
    }

    public void submit() {
        config.remove(KubernetesConfig.CLIENT_MASTER_URL);
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("job", config);
        String clientArgs = JSON.toJSONString(clientConfig);
        config.put(KubernetesConfigKeys.USER_MAIN_CLASS.getKey(),
            "org.apache.geaflow.example.k8s.UnBoundedStreamWordCount");
        config.put(KubernetesConfigKeys.USER_CLASS_ARGS.getKey(), clientArgs);
        KubernetesJobClient jobClient = new KubernetesJobClient(config, MASTER_URL);
        jobClient.submitJob();
    }

    public static void main(String[] args) {
        StreamWordCountTest test = new StreamWordCountTest();
        test.submit();
    }
}
