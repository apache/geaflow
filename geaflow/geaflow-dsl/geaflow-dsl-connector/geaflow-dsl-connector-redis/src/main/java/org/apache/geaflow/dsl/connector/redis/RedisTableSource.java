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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class RedisTableSource implements TableSource {

    private Configuration conf;
    private StructType schema;
    private RedisDataType dataType;
    private String keyPattern;
    private int scanCount;
    private String keyField;
    private String valueField;
    private String hashFieldField;
    private String hashValueField;
    private transient JedisPool jedisPool;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.conf = tableConf;
        this.schema = tableSchema;
        this.dataType = RedisDataType.of(
            tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_DATA_TYPE));
        this.keyPattern = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_KEY_PATTERN);
        this.scanCount = tableConf.getInteger(RedisConfigKeys.GEAFLOW_DSL_REDIS_SCAN_COUNT);
        this.keyField = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_KEY_FIELD);
        this.valueField = tableConf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_VALUE_FIELD);
        this.hashFieldField = tableConf.getString(
            RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_FIELD_FIELD);
        this.hashValueField = tableConf.getString(
            RedisConfigKeys.GEAFLOW_DSL_REDIS_HASH_VALUE_FIELD);
    }

    @Override
    public void open(RuntimeContext context) {
        this.jedisPool = RedisClient.createJedisPool(conf);
    }

    @Override
    public List<Partition> listPartitions() {
        return Collections.singletonList(new RedisPartition());
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            partitions.add(new RedisPartition(i, parallelism));
        }
        return partitions;
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new RedisTableDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        String cursor = startOffset.map(offset -> ((RedisOffset) offset).getCursor())
            .orElse(ScanParams.SCAN_POINTER_START);
        ScanParams scanParams = new ScanParams().match(keyPattern).count(scanCount);
        List<RedisRecord> records = new ArrayList<>();
        String nextCursor;
        try (Jedis jedis = jedisPool.getResource()) {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            nextCursor = scanResult.getCursor();
            RedisPartition redisPartition = (RedisPartition) partition;
            for (String key : scanResult.getResult()) {
                if (redisPartition.accept(key)) {
                    records.addAll(readKey(jedis, key));
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to fetch data from Redis", e);
        }
        boolean finish = ScanParams.SCAN_POINTER_START.equals(nextCursor);
        return (FetchData<T>) FetchData.createStreamFetch((List<T>) records,
            new RedisOffset(nextCursor), finish);
    }

    @Override
    public void close() {
        if (Objects.nonNull(jedisPool)) {
            jedisPool.close();
        }
    }

    private List<RedisRecord> readKey(Jedis jedis, String key) {
        if (dataType == RedisDataType.STRING) {
            String value = jedis.get(key);
            return Collections.singletonList(
                new RedisRecord(key, null, value, RedisDataType.STRING));
        }
        List<RedisRecord> records = new ArrayList<>();
        for (java.util.Map.Entry<String, String> entry : jedis.hgetAll(key).entrySet()) {
            records.add(new RedisRecord(key, entry.getKey(), entry.getValue(), RedisDataType.HASH));
        }
        return records;
    }

    private class RedisTableDeserializer implements TableDeserializer<RedisRecord> {

        @Override
        public void init(Configuration conf, StructType schema) {
        }

        @Override
        public List<Row> deserialize(RedisRecord record) {
            Object[] values = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                String fieldName = schema.getField(i).getName();
                String value = fieldValue(record, fieldName);
                values[i] = RedisRowConverter.fromRedisString(value, schema.getType(i));
            }
            return Collections.singletonList(ObjectRow.create(values));
        }
    }

    private String fieldValue(RedisRecord record, String fieldName) {
        if (fieldName.equalsIgnoreCase(keyField)) {
            return record.getKey();
        }
        if (dataType == RedisDataType.STRING && fieldName.equalsIgnoreCase(valueField)) {
            return record.getValue();
        }
        if (dataType == RedisDataType.HASH) {
            if (!hashFieldField.isEmpty() && fieldName.equalsIgnoreCase(hashFieldField)) {
                return record.getField();
            }
            if (!hashValueField.isEmpty() && fieldName.equalsIgnoreCase(hashValueField)) {
                return record.getValue();
            }
            if (fieldName.equalsIgnoreCase(record.getField())) {
                return record.getValue();
            }
        }
        if ("redis_type".equalsIgnoreCase(fieldName)) {
            return record.getDataType().name().toLowerCase();
        }
        throw new GeaFlowDSLException("Cannot map redis record to field: " + fieldName);
    }

    public static class RedisRecord {

        private final String key;
        private final String field;
        private final String value;
        private final RedisDataType dataType;

        public RedisRecord(String key, String field, String value, RedisDataType dataType) {
            this.key = key;
            this.field = field;
            this.value = value;
            this.dataType = dataType;
        }

        public String getKey() {
            return key;
        }

        public String getField() {
            return field;
        }

        public String getValue() {
            return value;
        }

        public RedisDataType getDataType() {
            return dataType;
        }
    }

    public static class RedisPartition implements Partition {

        private int index;
        private int parallel;

        public RedisPartition() {
            this(0, 1);
        }

        public RedisPartition(int index, int parallel) {
            this.index = index;
            this.parallel = parallel;
        }

        @Override
        public String getName() {
            return "redis-" + index + "-" + parallel;
        }

        @Override
        public void setIndex(int index, int parallel) {
            this.index = index;
            this.parallel = parallel;
        }

        public boolean accept(String key) {
            return Math.floorMod(key.hashCode(), parallel) == index;
        }
    }

    public static class RedisOffset implements Offset {

        private final String cursor;

        public RedisOffset(String cursor) {
            this.cursor = cursor;
        }

        public String getCursor() {
            return cursor;
        }

        @Override
        public String humanReadable() {
            return "RedisOffset{cursor='" + cursor + "'}";
        }

        @Override
        public long getOffset() {
            return Long.parseLong(cursor);
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
