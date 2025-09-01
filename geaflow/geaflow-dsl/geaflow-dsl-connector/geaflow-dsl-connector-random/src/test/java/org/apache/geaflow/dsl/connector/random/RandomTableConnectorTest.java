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

package org.apache.geaflow.dsl.connector.random;

import java.io.IOException;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RandomTableConnectorTest {

    @Test
    public void testRandom() throws IOException {
        RandomTableConnector connector = new RandomTableConnector();
        TableSource source = connector.createSource(new Configuration());
        Assert.assertEquals(source.getClass(), RandomTableSource.class);
        Configuration tableConf = new Configuration();

        source.init(tableConf, new TableSchema());

        source.open(new RuntimeContext() {
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

        source.listPartitions();
        source.close();
    }
}
