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

package org.apache.geaflow.example.stream;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.RecoverableFileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.callback.ICallbackFunction;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWordCountCallBackPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamWordCountCallBackPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/wordcount";
    public static final String REF_FILE_PATH = "data/reference/wordcount";
    private static final List<Long> taskCallBackList = new ArrayList<>();

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        TaskCallBack taskCallBack = pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource = pipelineTaskCxt.buildSource(
                        new RecoverableFileSource<String>("data/input/email_edge",
                            line -> {
                                String[] fields = line.split(",");
                                return Collections.singletonList(fields[0]);
                            }) {
                        }, SizeTumblingWindow.of(5000))
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .map(e -> Tuple.of(e, 1))
                    .keyBy(e -> e.f0)
                    .reduce((x, y) -> Tuple.of(x.f0, x.f1 + y.f1))
                    .withParallelism(conf.getInteger(ExampleConfigKeys.REDUCE_PARALLELISM))
                    .map(v -> String.format("%s,%s", ((Tuple) v).f0, ((Tuple) v).f1))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
            }
        });

        taskCallBackList.clear();
        taskCallBack.addCallBack(new ICallbackFunction() {
            @Override
            public void window(long windowId) {
                LOGGER.info("finish windowId:{}", windowId);
                taskCallBackList.add(windowId);
            }

            @Override
            public void terminal() {

            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
        Assert.assertTrue("task call back should handle window", !taskCallBackList.isEmpty());
    }
}
