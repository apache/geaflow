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

package org.apache.geaflow.env.args;

import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EnvironmentArgumentParserTest {

    @Test
    public void testConsoleArgs() {
        String args = "{\n" +
            "    \"job\":\n" +
            "    {\n" +
            "        \"geaflow.state.write.async.enable\": \"true\",\n" +
            "        \"geaflow.fo.enable\": false,\n" +
            "        \"geaflow.fo.max.restarts\": 0,\n" +
            "        \"geaflow.batch.number.per.checkpoint\": 1\n" +
            "    },\n" +
            "    \"system\":\n" +
            "    {\n" +
            "    \t\"geaflow.job.runtime.name\": \"geaflow123\",\n" +
            "    \t\"geaflow.job.unique.id\": \"123\",\n" +
            "    \t\"geaflow.job.id\": \"123456\",\n" +
            "        \"geaflow.job.owner\": \"test\",\n" +
            "        \"stateConfig\":\n" +
            "        {\n" +
            "            \"geaflow.file.persistent.root\": \"/geaflow/chk\",\n" +
            "            \"geaflow.file.persistent.config.json\":\n" +
            "            {\n" +
            "                \"fs.defaultFS\": \"dfs://xxxxxx\",\n" +
            "                \"dfs.usergroupservice.impl\": \"xxxxxx.class\",\n" +
            "                \"fs.AbstractFileSystem.dfs.impl\": \"xxxxxx\",\n" +
            "                \"ipc.client.connection.maxidletime\": \"300000\",\n" +
            "                \"alidfs.default.write.buffer.size\": \"1048576\",\n" +
            "                \"alidfs.default.read.buffer.size\": \"1048576\",\n" +
            "                \"alidfs.perf.counter.enable\": \"false\",\n" +
            "                \"fs.dfs.impl\": \"xxxxxx\"\n" +
            "            },\n" +
            "            \"geaflow.store.redis.host\": \"xxxxxx\",\n" +
            "            \"geaflow.file.persistent.type\": \"DFS\",\n" +
            "            \"geaflow.file.persistent.user.name\": \"geaflow\",\n" +
            "            \"geaflow.store.redis.port\": 8016\n" +
            "        },\n" +
            "        \n" +
            "        \"geaflow.cluster.started.callback.url\": \"http://xxxxxx\",\n" +
            "        \"metricConfig\":\n" +
            "        {\n" +
            "            \"geaflow.metric.reporters\": \"influxdb\",\n" +
            "            \"geaflow.metric.influxdb.url\": \"http://xxxxxx\",\n" +
            "            \"geaflow.metric.influxdb.bucket\": \"geaflow_metric\",\n" +
            "            \"geaflow.metric.influxdb.token\": \"xxxxxx\",\n" +
            "            \"geaflow.metric.influxdb.org\": \"geaflow\"\n" +
            "        },\n" +
            "        \"geaflow.gw.endpoint\": \"http://xxxxxx\",\n" +
            "        \"geaflow.job.cluster.id\": \"geaflow123-1684396791903\"\n" +
            "    },\n" +
            "    \"cluster\":\n" +
            "    {\n" +
            "        \"geaflow.container.memory.mb\": 20000,\n" +
            "        \"geaflow.system.state.backend.type\": \"ROCKSDB\",\n" +
            "        \"geaflow.container.worker.num\": 4,\n" +
            "        \"geaflow.container.num\": 1,\n" +
            "        \"geaflow.container.jvm.options\": \"-Xmx15000m,-Xms15000m,-Xmn10000m\",\n" +
            "        \"geaflow.container.vcores\": 8\n" +
            "    }\n" +
            "}";

        EnvironmentArgumentParser parser = new EnvironmentArgumentParser();
        Map<String, String> config = parser.parse(new String[]{args});
        Assert.assertNotNull(config);

        Map<String, String> escapeConfig =
            parser.parse(new String[]{StringEscapeUtils.escapeJava(args)});
        Assert.assertNotNull(escapeConfig);
    }

}
