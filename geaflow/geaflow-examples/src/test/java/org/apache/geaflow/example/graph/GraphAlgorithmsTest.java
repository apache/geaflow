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

package org.apache.geaflow.example.graph;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.graph.statical.compute.allshortestpath.AllShortestPath;
import org.apache.geaflow.example.graph.statical.compute.closenesscentrality.ClosenessCentrality;
import org.apache.geaflow.example.graph.statical.compute.clustercoefficient.ClusterCoefficient;
import org.apache.geaflow.example.graph.statical.compute.commonneighbors.CommonNeighbors;
import org.apache.geaflow.example.graph.statical.compute.kcore.KCore;
import org.apache.geaflow.example.graph.statical.compute.labelpropagation.LabelPropagation;
import org.apache.geaflow.example.graph.statical.compute.linkprediction.LinkPrediction;
import org.apache.geaflow.example.graph.statical.compute.npaths.NPaths;
import org.apache.geaflow.example.graph.statical.compute.pagerank.PageRank;
import org.apache.geaflow.example.graph.statical.compute.personalrank.PersonalRank;
import org.apache.geaflow.example.graph.statical.compute.shortestpath.ShortestPath;
import org.apache.geaflow.example.graph.statical.compute.shortestpathofvertexsets.ShortestPathOfVertexSet;
import org.apache.geaflow.example.graph.statical.compute.sssp.SSSP;
import org.apache.geaflow.example.graph.statical.compute.weakconnectedcomponents.WeakConnectedComponents;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class GraphAlgorithmsTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphAlgorithmsTest.class);

    private static final Map<String, String> TEST_CONFIG = new HashMap<>();

    static {
        TEST_CONFIG.put(ExampleConfigKeys.SOURCE_PARALLELISM.getKey(), String.valueOf(3));
        TEST_CONFIG.put(ExampleConfigKeys.ITERATOR_PARALLELISM.getKey(), String.valueOf(4));
        TEST_CONFIG.put(ExampleConfigKeys.SINK_PARALLELISM.getKey(), String.valueOf(5));
        TEST_CONFIG.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());
    }

    public static class GraphAlgorithmTestFactory {
        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new GraphAlgorithmsTest(true),
                new GraphAlgorithmsTest(false),
            };
        }
    }

    private final boolean memoryPool;

    public GraphAlgorithmsTest(boolean memoryPool) {
        this.memoryPool = memoryPool;
    }

    @Test
    public void weakConnectedComponentsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = WeakConnectedComponents.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        WeakConnectedComponents.validateResult();
    }


    @Test
    public void allShortestPathTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = AllShortestPath.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        AllShortestPath.validateResult();
    }

    @Test
    public void closenessCentralityTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = ClosenessCentrality.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ClosenessCentrality.validateResult();
    }

    @Test
    public void clusterCoefficientTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = ClusterCoefficient.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ClusterCoefficient.validateResult();
    }

    @Test
    public void commonNeighborsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = CommonNeighbors.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        CommonNeighbors.validateResult();
    }

    @Test
    public void kCoreTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = KCore.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        KCore.validateResult();
    }

    @Test
    public void labelPropagationTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = LabelPropagation.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        LabelPropagation.validateResult();
    }


    @Test
    public void linkPredictionTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = LinkPrediction.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        LinkPrediction.validateResult();
    }

    @Test
    public void nPathsTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = NPaths.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        NPaths.validateResult();
    }

    @Test
    public void pageRankTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = PageRank.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        PageRank.validateResult();
    }

    @Test
    public void personalRankTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = PersonalRank.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        PersonalRank.validateResult();
    }

    @Test
    public void shortestPathTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = ShortestPath.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ShortestPath.validateResult();
    }

    @Test
    public void shortestPathOfVertexSetTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = ShortestPathOfVertexSet.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        ShortestPathOfVertexSet.validateResult();
    }

    @Test
    public void SSSPTest() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.putAll(TEST_CONFIG);
        config.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE.getKey(),
            Boolean.toString(memoryPool));

        IPipelineResult result = SSSP.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        SSSP.validateResult();
    }

}
