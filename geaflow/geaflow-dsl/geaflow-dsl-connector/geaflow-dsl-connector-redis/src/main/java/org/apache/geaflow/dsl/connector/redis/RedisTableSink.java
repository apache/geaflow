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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTableSink.class);

    private Configuration conf;
    private StructType schema;
    private RedisDataType dataType;
    private String keyField;
    private String valueField;
    private String hashFieldField;
    private String hashValueField;
    private transient JedisPool jedisPool;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("Prepare redis sink with config: {}, schema: {}", tableConf, schema);
        this.conf = tableConf;
        this.schema = schema;
        this.dataType = RedisDataType.of(
            tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_DATA_TYPE));
        this.keyField = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_KEY_FIELD);
        this.valueField = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_VALUE_FIELD);
        this.hashFieldField = tableConf.getString(
            RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_FIELD_FIELD);
        this.hashValueField = tableConf.getString(
            RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_VALUE_FIELD);
        requireField(keyField);
        if (dataType == RedisDataType.STRING) {
            requireField(valueField);
        } else if (!hashFieldField.isEmpty() || !hashValueField.isEmpty()) {
            requireField(hashFieldField);
            requireField(hashValueField);
        }
    }

    @Override
    public void open(RuntimeContext context) {
        this.jedisPool = RedisClient.createJedisPool(conf);
    }

    @Override
    public void write(Row row) throws IOException {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = fieldAsString(row, keyField);
            if (key == null) {
                throw new GeaFlowDSLException("Redis key field can not be null: " + keyField);
            }
            if (dataType == RedisDataType.STRING) {
                jedis.set(key, fieldAsString(row, valueField));
            } else {
                writeHash(jedis, key, row);
            }
        } catch (Exception e) {
            throw new IOException("Failed to write row to Redis", e);
        }
    }

    @Override
    public void finish() {
    }

    @Override
    public void close() {
        if (Objects.nonNull(jedisPool)) {
            jedisPool.close();
        }
    }

    private void writeHash(Jedis jedis, String key, Row row) {
        if (!hashFieldField.isEmpty()) {
            String field = fieldAsString(row, hashFieldField);
            String value = fieldAsString(row, hashValueField);
            jedis.hset(key, field, value);
            return;
        }
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            String fieldName = schema.getField(i).getName();
            if (!fieldName.equalsIgnoreCase(keyField)) {
                values.put(fieldName, RedisRowConverter.toRedisString(
                    row.getField(i, schema.getType(i))));
            }
        }
        if (!values.isEmpty()) {
            jedis.hset(key, values);
        }
    }

    private String fieldAsString(Row row, String fieldName) {
        int index = schema.indexOf(fieldName);
        return RedisRowConverter.toRedisString(row.getField(index, schema.getType(index)));
    }

    private void requireField(String fieldName) {
        if (fieldName == null || fieldName.isEmpty() || schema.indexOf(fieldName) < 0) {
            throw new GeaFlowDSLException("Redis connector requires field: " + fieldName);
        }
    }
}
