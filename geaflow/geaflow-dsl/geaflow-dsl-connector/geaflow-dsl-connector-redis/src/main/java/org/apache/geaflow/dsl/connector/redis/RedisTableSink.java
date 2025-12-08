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

import java.io.IOException;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTableSink implements TableSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTableSink.class);

    private Configuration tableConf;
    private StructType schema;
    private transient JedisPool jedisPool;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        this.tableConf = tableConf;
        this.schema = schema;
    }

    @Override
    public void open(RuntimeContext context) {
        String host = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_HOST);
        int port = tableConf.getInteger(RedisConfigKeys.GEAFLOW_DSL_REDIS_PORT);
        String password = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_PASSWORD, (String) null);
        int dbIndex = tableConf.getInteger(RedisConfigKeys.GEAFLOW_DSL_REDIS_DB_INDEX);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);

        if (password != null && !password.isEmpty()) {
            this.jedisPool = new JedisPool(poolConfig, host, port, 2000, password, dbIndex);
        } else {
            this.jedisPool = new JedisPool(poolConfig, host, port, 2000, null, dbIndex);
        }
        LOGGER.info("Redis sink opened. Host: {}, Port: {}", host, port);
    }

    @Override
    public void write(Row row) throws IOException {
        if (row == null || schema.getFields().size() < 2) {
            return;
        }
        // Assume first column is key
        String key = String.valueOf(row.getField(0, schema.getType(0)));

        try (Jedis jedis = jedisPool.getResource()) {
            // Write other columns as hash fields
            for (int i = 1; i < schema.getFields().size(); i++) {
                String field = schema.getField(i).getName();
                Object value = row.getField(i, schema.getType(i));
                if (value != null) {
                    jedis.hset(key, field, String.valueOf(value));
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to write to Redis", e);
        }
    }

    @Override
    public void finish() throws IOException {
        // No-op for Redis sink as operations are immediate
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
        LOGGER.info("Redis sink closed.");
    }
}
