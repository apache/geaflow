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

package org.apache.geaflow.example.base;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

    protected Map<String, String> config;
    protected Environment environment;

    @BeforeMethod
    public void cleanMetaStore() {
        LOGGER.info("clean cluster meta store");
        ClusterMetaStore.close();
        environment = null;
    }

    @BeforeMethod
    public void setup() {
        config = new HashMap<>();
        config.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());
        config.put(ExecutionConfigKeys.HTTP_REST_SERVICE_ENABLE.getKey(), "false");
    }

    @AfterMethod
    public void clean() {
        if (environment != null) {
            environment.shutdown();
        }
        ClusterMetaStore.close();
        ScheduledWorkerManagerFactory.clear();
    }

    public Map<String, String> getConfig() {
        return config;
    }
}
