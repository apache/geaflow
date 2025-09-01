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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.WindowFactory;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;

public class WindowStreamWordCount {

    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onRayEnvironment(args);
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration config = pipelineTaskCxt.getConfig();
                List<String> words = Lists.newArrayList("hello", "world", "hello", "word");
                PWindowSource<String> streamSource =
                    pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {},
                        SizeTumblingWindow.of(100));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(config);
                streamSource.window(WindowFactory.createSizeTumblingWindow(2))
                    .sink(sink);
            }
        });

        pipeline.execute();
    }
}
