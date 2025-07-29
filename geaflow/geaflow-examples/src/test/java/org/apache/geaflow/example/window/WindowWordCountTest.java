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

package org.apache.geaflow.example.window;

import static org.apache.geaflow.example.config.ExampleConfigKeys.REDUCE_PARALLELISM;
import static org.apache.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;
import static org.apache.geaflow.example.config.ExampleConfigKeys.SOURCE_PARALLELISM;

import java.util.Comparator;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class WindowWordCountTest extends BaseTest {

    public static class TestWindowWordCountFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new WindowWordCountTest(true),
                new WindowWordCountTest(false),
            };
        }

    }

    private final boolean prefetch;

    public WindowWordCountTest(boolean prefetch) {
        this.prefetch = prefetch;
    }

    @Test
    public void testSingleConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);
        configuration.put(ExecutionConfigKeys.SHUFFLE_PREFETCH, String.valueOf(this.prefetch));

        WindowWordCountPipeline pipeline = new WindowWordCountPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult(new DataComparator());

    }

    @Test
    public void testReduceTwoAndSinkFourConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.SHUFFLE_PREFETCH, String.valueOf(this.prefetch));

        WindowWordCountPipeline pipeline = new WindowWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult(new DataComparator());

    }

    @Test
    public void testReduceOneAndSinkFourConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.SHUFFLE_PREFETCH, String.valueOf(this.prefetch));

        WindowWordCountPipeline pipeline = new WindowWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "1");
        config.put(SINK_PARALLELISM.getKey(), "4");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult(new DataComparator());

    }

    @Test
    public void testReduceTwoAndSinkTwoConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.SHUFFLE_PREFETCH, String.valueOf(this.prefetch));

        WindowWordCountPipeline pipeline = new WindowWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "2");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult(new DataComparator());

    }

    @Test
    public void testReduceTwoAndSinkOneConcurrency() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.SHUFFLE_PREFETCH, String.valueOf(this.prefetch));

        WindowWordCountPipeline pipeline = new WindowWordCountPipeline();
        config.put(SOURCE_PARALLELISM.getKey(), "1");
        config.put(REDUCE_PARALLELISM.getKey(), "2");
        config.put(SINK_PARALLELISM.getKey(), "1");
        configuration.putAll(config);
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult(new DataComparator());

    }

    static class DataComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            if (o1 != null && o2 != null) {
                String[] data1 = o1.replace(")", "").split(",");
                String[] data2 = o2.replace(")", "").split(",");

                if (!data1[0].equals(data2[0])) {
                    return o1.compareTo(o2);
                } else {
                    return Integer.valueOf(data1[1]).compareTo(Integer.valueOf(data2[1]));
                }
            }

            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof String) {
                return obj.equals(this);
            }
            return false;
        }
    }

}
