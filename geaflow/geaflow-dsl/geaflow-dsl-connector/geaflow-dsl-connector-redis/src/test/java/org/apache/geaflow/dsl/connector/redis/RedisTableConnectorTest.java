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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.github.fppt.jedismock.RedisServer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

public class RedisTableConnectorTest {

    private RedisServer redisServer;
    private Configuration conf;

    @BeforeMethod
    public void setUp() throws Exception {
        redisServer = RedisServer.newRedisServer().start();
        conf = new Configuration();
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_HOST, redisServer.getHost());
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_KEY_PATTERN, "test:*");
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_SCAN_COUNT, "100");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        redisServer.stop();
    }

    @Test
    public void testStringSinkAndSource() throws Exception {
        TableSchema schema = new TableSchema(
            new TableField("redis_key", Types.STRING),
            new TableField("redis_value", Types.STRING));

        RedisTableSink sink = new RedisTableSink();
        sink.init(conf, schema);
        sink.open(null);
        sink.write(ObjectRow.create("test:string:1", "hello"));
        sink.close();

        try (Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort())) {
            assertEquals(jedis.get("test:string:1"), "hello");
        }

        RedisTableSource source = new RedisTableSource();
        source.init(conf, schema);
        source.open(null);
        List<Row> rows = readRows(source, schema);
        source.close();

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getField(0, Types.STRING), "test:string:1");
        assertEquals(rows.get(0).getField(1, Types.STRING), "hello");
    }

    @Test
    public void testHashEntrySinkAndSource() throws Exception {
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_DATA_TYPE, "hash");
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_FIELD_FIELD, "hash_field");
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_VALUE_FIELD, "hash_value");
        TableSchema schema = new TableSchema(
            new TableField("redis_key", Types.STRING),
            new TableField("hash_field", Types.STRING),
            new TableField("hash_value", Types.STRING));

        RedisTableSink sink = new RedisTableSink();
        sink.init(conf, schema);
        sink.open(null);
        sink.write(ObjectRow.create("test:hash:1", "name", "geaflow"));
        sink.write(ObjectRow.create("test:hash:1", "version", "0.8.0"));
        sink.close();

        try (Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort())) {
            assertEquals(jedis.hget("test:hash:1", "name"), "geaflow");
            assertEquals(jedis.hget("test:hash:1", "version"), "0.8.0");
        }

        RedisTableSource source = new RedisTableSource();
        source.init(conf, schema);
        source.open(null);
        List<Row> rows = readRows(source, schema);
        source.close();

        assertEquals(rows.size(), 2);
        assertTrue(rows.stream().anyMatch(row -> "name".equals(row.getField(1, Types.STRING))));
        assertTrue(rows.stream().anyMatch(row -> "version".equals(row.getField(1, Types.STRING))));
    }

    @Test
    public void testHashColumnSink() throws Exception {
        conf.put(RedisConfigKeys.GEAFLOW_DSL_REDIS_DATA_TYPE, "hash");
        TableSchema schema = new TableSchema(
            new TableField("redis_key", Types.STRING),
            new TableField("name", Types.STRING),
            new TableField("age", Types.INTEGER));

        RedisTableSink sink = new RedisTableSink();
        sink.init(conf, schema);
        sink.open(null);
        sink.write(ObjectRow.create("test:hash:2", "geaflow", 10));
        sink.close();

        try (Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort())) {
            assertEquals(jedis.hget("test:hash:2", "name"), "geaflow");
            assertEquals(jedis.hget("test:hash:2", "age"), "10");
        }
    }

    private List<Row> readRows(RedisTableSource source, TableSchema schema) throws Exception {
        List<Partition> partitions = source.listPartitions();
        FetchData<RedisTableSource.RedisRecord> fetchData = source.fetch(partitions.get(0),
            java.util.Optional.empty(), null);
        assertTrue(fetchData.isFinish());
        assertFalse(fetchData.getDataSize() < 0);
        TableDeserializer<RedisTableSource.RedisRecord> deserializer = source.getDeserializer(conf);
        deserializer.init(conf, schema);
        List<Row> rows = new ArrayList<>();
        Iterator<RedisTableSource.RedisRecord> iterator = fetchData.getDataIterator();
        while (iterator.hasNext()) {
            rows.addAll(deserializer.deserialize(iterator.next()));
        }
        return rows;
    }
}
