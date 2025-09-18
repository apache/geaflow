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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.MapFunction;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowWordCountPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowWordCountPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/count1";
    public static final String REF_FILE_PATH = "data/reference/count1";

    public IPipelineResult submit(Environment environment) {
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        envConfig.getConfigMap().put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE.getKey(), Boolean.TRUE.toString());
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource = pipelineTaskCxt.buildSource(
                        new FileSource<String>("data/input/email_edge",
                            line -> {
                                String[] fields = line.split(",");
                                return Collections.singletonList(fields[0]);
                            }) {
                        }, AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .map(e -> Tuple.of(e, 1))
                    .keyBy(new KeySelectorFunc())
                    .reduce(new CountFunc())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.REDUCE_PARALLELISM))
                    .map(v -> String.format("(%s,%s)", ((Tuple) v).f0,
                        ((Tuple) v).f1))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
            }
        });

        return pipeline.execute();
    }

    public static void validateResult(Comparator<String> comparator) throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH, comparator);
    }


    public static class MapFunc implements MapFunction<String, Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> map(String value) {
            LOGGER.info("MapFunc process value: {}", value);
            return Tuple.of(value, 1);
        }
    }

    public static class KeySelectorFunc implements KeySelector<Tuple<String, Integer>, Object> {

        @Override
        public Object getKey(Tuple<String, Integer> value) {
            return value.f0;
        }
    }

    public static class CountFunc implements ReduceFunction<Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> reduce(Tuple<String, Integer> oldValue, Tuple<String, Integer> newValue) {
            return Tuple.of(oldValue.f0, oldValue.f1 + newValue.f1);
        }
    }
}
