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

package org.apache.geaflow.example.graph.inc.traversal;

import static org.apache.geaflow.example.config.ExampleConfigKeys.ITERATOR_PARALLELISM;
import static org.apache.geaflow.example.config.ExampleConfigKeys.MAP_PARALLELISM;
import static org.apache.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;
import static org.apache.geaflow.example.config.ExampleConfigKeys.SOURCE_PARALLELISM;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.example.graph.dynamic.traversal.IncrGraphTraversalByStartIds;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncGraphTraversalByStartIdsTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphTraversalByStartIdsTest.class);

    private Map<String, String> config;

    @BeforeMethod
    public void setUp() {
        config = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), getClass().getSimpleName());
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/");
        String path = config.get(FileConfigKeys.ROOT.getKey()) + config.get(ExecutionConfigKeys.JOB_APP_NAME.getKey());
        FileUtils.deleteQuietly(new File(path));
    }

    @Test
    public void test1ShardWithSingleConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCMapOneSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCFourMapSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(MAP_PARALLELISM.getKey(), String.valueOf(4));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCMapFourSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();

        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));

        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test1ShardWithOneSourceVCMapFourSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(1));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(4));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithTwoSourceVCOneMapTwoSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(2));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(2));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCOneMapTwoSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(2));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }

    @Test
    public void test2ShardWithOneSourceVCMapSinkConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        IncrGraphTraversalByStartIds pipeline = new IncrGraphTraversalByStartIds();
        config.put(SOURCE_PARALLELISM.getKey(), String.valueOf(1));
        config.put(ITERATOR_PARALLELISM.getKey(), String.valueOf(2));
        config.put(SINK_PARALLELISM.getKey(), String.valueOf(1));
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        result.get();
        pipeline.validateResult();
    }
}
