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

package org.apache.geaflow.dsl.connector.redis;

import com.github.fppt.jedismock.RedisServer;
import java.io.IOException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.common.type.Types;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

public class RedisTableSinkTest {

    private RedisServer server;
    private RedisTableSink sink;

    @BeforeMethod
    public void setUp() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();

        sink = new RedisTableSink();
        Configuration conf = new Configuration();
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_HOST, server.getHost());
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_PORT, String.valueOf(server.getBindPort()));

        StructType schema = new StructType(
            new TableField("id", Types.INTEGER, false),
            new TableField("name", Types.STRING, false),
            new TableField("age", Types.INTEGER, false)
        );

        sink.init(conf, schema);
        sink.open(null);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (sink != null) {
            sink.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testWrite() throws IOException {
        Row row = ObjectRow.create(1, "alice", 20);
        sink.write(row);

        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            String name = jedis.hget("1", "name");
            String age = jedis.hget("1", "age");

            Assert.assertEquals(name, "alice");
            Assert.assertEquals(age, "20");
        }
    }
}
