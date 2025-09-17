package org.apache.geaflow.dsl.runtime.connector;/*
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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.dsl.common.compile.CompileContext;
import org.apache.geaflow.dsl.common.compile.QueryCompiler;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectorCompilerTest {

    @Test
    public void testFindUnResolvedPlugins() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "CREATE GRAPH IF NOT EXISTS dy_modern (\n"
            + "  Vertex person (\n"
            + "    id bigint ID,\n"
            + "    name varchar\n"
            + "  ),\n"
            + "  Edge knows (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    weight double\n"
            + "  )\n"
            + ") WITH (\n"
            + "  storeType='rocksdb',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "\n"
            + "CREATE TABLE hive (\n"
            + "  id BIGINT,\n"
            + "  name VARCHAR,\n"
            + "  age INT\n"
            + ") WITH (\n"
            + "    type='hive',\n"
            + "    geaflow.dsl.kafka.servers = 'localhost:9092',\n"
            + "    geaflow.dsl.kafka.topic = 'read-topic'\n"
            + ");\n"
            + "\n"
            + "CREATE TABLE kafka_sink (\n"
            + "  id BIGINT,\n"
            + "  name VARCHAR,\n"
            + "  age INT\n"
            + ") WITH (\n"
            + "    type='kafka',\n"
            + "    geaflow.dsl.kafka.servers = 'localhost:9092',\n"
            + "    geaflow.dsl.kafka.topic = 'write-topic'\n"
            + ");\n"
            + "\n"
            + "CREATE TABLE kafka_123(\n"
            + "  id BIGINT,\n"
            + "  name VARCHAR,\n"
            + "  age INT\n"
            + ") WITH (\n"
            + "    type='kafka123',\n"
            + "    geaflow.dsl.kafka.servers = 'localhost:9092',\n"
            + "    geaflow.dsl.kafka.topic = 'write-topic'\n"
            + ");\n"
            + "\n"
            + "INSERT INTO kafka_sink\n"
            + "SELECT * FROM kafka_source;";

        Set<String> plugins = compiler.getDeclaredTablePlugins(script, context);
        Set<String> enginePlugins = compiler.getEnginePlugins();
        Assert.assertEquals(plugins.size(), 3);
        List<String> filteredSet = plugins.stream().filter(e -> !enginePlugins.contains(e.toUpperCase()))
            .collect(Collectors.toList());
        Assert.assertEquals(filteredSet.size(), 1);

        Assert.assertEquals(filteredSet.get(0), "kafka123");
    }
}
