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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.example.stream.StreamWordCountCallBackPipeline;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class IncrWordCountCallBackTest extends BaseTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(IncrWordCountCallBackTest.class);

    @Test
    public void testTaskCallBack() throws Exception {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);

        StreamWordCountCallBackPipeline pipeline = new StreamWordCountCallBackPipeline();
        IPipelineResult result = pipeline.submit(environment);
        if (!result.isSuccess()) {
            throw new Exception("execute failed");
        }
        pipeline.validateResult();

    }
}
