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

package org.apache.geaflow.example.window.incremental;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.stream.StreamKeyAggPipeline;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncrKeyAggTest extends BaseTest {

    private Map<String, String> config;

    @BeforeMethod
    public void setUp() {
        config = new HashMap<>();
    }

    @Test
    public void test() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);

        StreamKeyAggPipeline pipeline = new StreamKeyAggPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void test1() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        configuration.putAll(config);

        StreamKeyAggPipeline pipeline = new StreamKeyAggPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void test2() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "3");
        configuration.putAll(config);

        StreamKeyAggPipeline pipeline = new StreamKeyAggPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void test3() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "2");
        configuration.putAll(config);

        StreamKeyAggPipeline pipeline = new StreamKeyAggPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

    @Test
    public void test4() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        config.put(ExampleConfigKeys.AGG_PARALLELISM.getKey(), "3");
        config.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);

        StreamKeyAggPipeline pipeline = new StreamKeyAggPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();
    }

}
