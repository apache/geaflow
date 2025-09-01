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

package org.apache.geaflow.dsl.connector.console;

import java.io.IOException;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConsoleTableConnectorTest {

    @Test
    public void testConsole() throws IOException {
        ConsoleTableConnector connector = new ConsoleTableConnector();
        TableSink sink = connector.createSink(new Configuration());
        Assert.assertEquals(sink.getClass(), ConsoleTableSink.class);
        Configuration tableConf = new Configuration();

        sink.init(tableConf, new StructType());

        sink.open(new RuntimeContext() {
            @Override
            public long getPipelineId() {
                return 0;
            }

            @Override
            public String getPipelineName() {
                return null;
            }

            @Override
            public TaskArgs getTaskArgs() {
                return null;
            }

            @Override
            public Configuration getConfiguration() {
                return null;
            }

            @Override
            public String getWorkPath() {
                return null;
            }

            @Override
            public MetricGroup getMetric() {
                return null;
            }

            @Override
            public RuntimeContext clone(Map<String, String> opConfig) {
                return null;
            }

            @Override
            public long getWindowId() {
                return 0;
            }
        });

        sink.write(ObjectRow.create(1, 2, 3));
        sink.finish();
        sink.close();
    }
}
