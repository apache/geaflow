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

package org.apache.geaflow.example.util;

import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.ConsoleSink;
import org.apache.geaflow.example.function.FileSink;

public class ExampleSinkFunctionFactory {

    public enum SinkType {

        /**
         * result sink to console.
         */
        CONSOLE_SINK,
        /**
         * result sink to local file.
         */
        FILE_SINK
    }

    public static <T> SinkFunction<T> getSinkFunction(Configuration configuration) {
        String sinkType = configuration.getString(ExampleConfigKeys.GEAFLOW_SINK_TYPE);
        if (sinkType.equalsIgnoreCase(SinkType.CONSOLE_SINK.name())) {
            return new ConsoleSink<>();
        }
        return new FileSink<>();
    }
}
