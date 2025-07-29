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

package org.apache.geaflow.example.service;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_WORKER_NUM;

import com.github.fppt.jedismock.RedisServer;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.base.BaseQueryTest;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.metaserver.MetaServer;
import org.apache.geaflow.metaserver.MetaServerContext;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.apache.geaflow.store.redis.RedisConfigKeys;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

public class BaseServiceTest {

    protected static final String TEST_GRAPH_PATH = "/tmp/geaflow/analytics/test/graph";
    protected static final String HOST_NAME = "localhost";
    protected static final int DEFAULT_WAITING_TIME = 5;
    protected RedisServer server;
    protected MetaServer metaServer;
    protected Configuration defaultConfig;
    protected Environment environment;

    public final String graphView =
        "CREATE GRAPH bi (\n"
            + "  --static\n"
            + "  --Place\n"
            + "  Vertex Country (\n"
            + "    id bigint ID,\n"
            + "    name varchar,\n"
            + "    url varchar\n"
            + "  ),\n"
            + "  Vertex City (\n"
            + "    id bigint ID,\n"
            + "    name varchar,\n"
            + "    url varchar\n"
            + "  ),\n"
            + "  Vertex Continent (\n"
            + "    id bigint ID,\n"
            + "    name varchar,\n"
            + "    url varchar\n"
            + "  ),\n"
            + "  --Organisation\n"
            + "  Vertex Company (\n"
            + "    id bigint ID,\n"
            + "    name varchar,\n"
            + "    url varchar\n"
            + "  ),\n"
            + "  Vertex University (\n"
            + "    id bigint ID,\n"
            + "    name varchar,\n"
            + "    url varchar\n"
            + "  ),\n"
            + "  --Tag\n"
            + "\tVertex TagClass (\n"
            + "\t  id bigint ID,\n"
            + "\t  name varchar,\n"
            + "\t  url varchar\n"
            + "\t),\n"
            + "\tVertex Tag (\n"
            + "\t  id bigint ID,\n"
            + "\t  name varchar,\n"
            + "\t  url varchar\n"
            + "\t),\n"
            + "\n"
            + "  --dynamic\n"
            + "  Vertex Person (\n"
            + "    id bigint ID,\n"
            + "    creationDate bigint,\n"
            + "    firstName varchar,\n"
            + "    lastName varchar,\n"
            + "    gender varchar,\n"
            + "    --birthday Date,\n"
            + "    --email {varchar},\n"
            + "    --speaks {varchar},\n"
            + "    browserUsed varchar,\n"
            + "    locationIP varchar\n"
            + "  ),\n"
            + "  Vertex Forum (\n"
            + "    id bigint ID,\n"
            + "    creationDate bigint,\n"
            + "    title varchar\n"
            + "  ),\n"
            + "  --Message\n"
            + "  Vertex Post (\n"
            + "    id bigint ID,\n"
            + "    creationDate bigint,\n"
            + "    browserUsed varchar,\n"
            + "    locationIP varchar,\n"
            + "    content varchar,\n"
            + "    length bigint,\n"
            + "    lang varchar,\n"
            + "    imageFile varchar\n"
            + "  ),\n"
            + "  Vertex Comment (\n"
            + "    id bigint ID,\n"
            + "    creationDate bigint,\n"
            + "    browserUsed varchar,\n"
            + "    locationIP varchar,\n"
            + "    content varchar,\n"
            + "    length bigint\n"
            + "  ),\n"
            + "\n"
            + "  --relations\n"
            + "  --static\n"
            + "\tEdge isLocatedIn (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "\tEdge isPartOf (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "  Edge isSubclassOf (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID\n"
            + "  ),\n"
            + "  Edge hasType (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID\n"
            + "  ),\n"
            + "\n"
            + "  --dynamic\n"
            + "\tEdge hasModerator (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "\tEdge containerOf (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "\tEdge replyOf (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "\tEdge hasTag (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID\n"
            + "\t),\n"
            + "  Edge hasInterest (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID\n"
            + "  ),\n"
            + "  Edge hasCreator (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID\n"
            + "  ),\n"
            + "  Edge workAt (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    workForm bigint\n"
            + "  ),\n"
            + "  Edge studyAt (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    classYear bigint\n"
            + "  ),\n"
            + "\n"
            + "  --temporary\n"
            + "  Edge hasMember (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    creationDate bigint\n"
            + "  ),\n"
            + "  Edge likes (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    creationDate bigint\n"
            + "  ),\n"
            + "  Edge knows (\n"
            + "    srcId bigint SOURCE ID,\n"
            + "    targetId bigint DESTINATION ID,\n"
            + "    creationDate bigint\n"
            + "  )\n"
            + ") WITH (\n"
            + "  \t\tstoreType='rocksdb',\n"
            + "    \tshardCount = 4\n"
            + " );\n"
            + "\n"
            + "USE GRAPH bi;";

    public final String analyticsQuery = graphView + "MATCH (a) RETURN a limit 0";
    public final String executeQuery =
        graphView + "MATCH (person:Person where id = 1100001)-[:isLocatedIn]->(city:City)\n"
            + "RETURN person.id, person.firstName, person.lastName";

    @BeforeClass
    public void beforeClass() throws Exception {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        BaseQueryTest
            .build()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "1")
            .withConfig(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS")
            .withConfig(CONTAINER_WORKER_NUM.getKey(), String.valueOf(20))
            .withConfig(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH)
            .withConfig(AnalyticsServiceConfigKeys.ANALYTICS_COMPILE_SCHEMA_ENABLE.getKey(),
                String.valueOf(false))
            .withConfig(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}")
            .withQueryPath("/ldbc/bi_insert_01.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_02.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_03.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_04.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_05.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_06.sql")
            .execute();
    }

    public void before() throws Exception {
        String jobName = "test_analytics_" + System.currentTimeMillis();
        server = RedisServer.newRedisServer().start();
        defaultConfig = new Configuration();
        defaultConfig.put(RedisConfigKeys.REDIS_HOST, server.getHost());
        defaultConfig.put(RedisConfigKeys.REDIS_PORT, String.valueOf(server.getBindPort()));
        defaultConfig.put(ExecutionConfigKeys.JOB_APP_NAME, jobName);
        metaServer = new MetaServer();
        metaServer.init(new MetaServerContext(defaultConfig));
    }

    @AfterClass
    public void cleanGraphStore() throws IOException {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @AfterMethod
    public void clean() throws IOException {
        if (metaServer != null) {
            metaServer.close();
        }
        if (server != null) {
            server.stop();
        }

        if (environment != null) {
            environment.shutdown();
            environment = null;
        }
        ClusterMetaStore.close();
        ScheduledWorkerManagerFactory.clear();
    }

    protected static ManagedChannel buildChannel(String host, int port, int timeoutMs) {
        return NettyChannelBuilder.forAddress(host, port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build();
    }
}
