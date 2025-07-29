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

package org.apache.geaflow.dsl.connector.api.function;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.rocksdb.RocksdbStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The offset store for {@link TableSource} to read and write offset for each partition.
 */
public class OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetStore.class);

    private static final String KEY_SEPARATOR = "_";
    private static final String CONSOLE_OFFSET = "offset";
    private static final String CHECKPOINT_OFFSET = "checkpoint" + KEY_SEPARATOR + "offset";

    private final long bucketNum;
    private final String jobId;
    private final String tableName;

    private final transient IKVStore<String, Offset> kvStore;
    private final transient Map<String, Offset> kvStoreCache;
    private final transient IKVStore<String, String> jsonOffsetStore;

    public OffsetStore(RuntimeContext runtimeContext, String tableName) {
        Configuration configuration = runtimeContext.getConfiguration();
        jobId = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.tableName = Objects.requireNonNull(tableName);

        String backendType = runtimeContext.getConfiguration().getString(FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE);
        IStoreBuilder builder = StoreBuilderFactory.build(backendType.toUpperCase(Locale.ROOT));
        if (builder instanceof RocksdbStoreBuilder) {
            throw new GeaflowRuntimeException("GeaFlow offset not support ROCKSDB storage and should "
                + "be configured as JDBC or MEMORY");
        }
        kvStore = (IKVStore<String, Offset>) builder.getStore(DataModel.KV, configuration);
        jsonOffsetStore = (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        String stateName = configuration.getString(ExecutionConfigKeys.SYSTEM_META_TABLE,
            generateKey(jobId, tableName));
        StoreContext storeContext = new StoreContext(stateName).withConfig(configuration);
        storeContext.withKeySerializer(new OffsetKvSerializer());
        kvStore.init(storeContext);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, String.class));
        jsonOffsetStore.init(storeContext);

        this.bucketNum = 2 * runtimeContext.getConfiguration().getLong(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT);
        this.kvStoreCache = new HashMap<>();
        LOGGER.info("init offset store, store type is: {}", backendType);
    }

    public Offset readOffset(String partitionName, long batchId) {
        long bucketId = batchId % bucketNum;
        String key = generateKey(jobId, CHECKPOINT_OFFSET, tableName, partitionName,
            String.valueOf(bucketId));
        if (kvStoreCache.containsKey(key)) {
            return kvStoreCache.get(key);
        } else {
            Offset offset = RetryCommand.run(() -> kvStore.get(key), 3);
            kvStoreCache.put(key, offset);
            return offset;
        }
    }

    public void writeOffset(String partitionName, long batchId, Offset offset) {
        long bucketId = batchId % bucketNum;
        String key = generateKey(jobId, CHECKPOINT_OFFSET, tableName, partitionName, String.valueOf(bucketId));
        String keyForConsole = generateKey(jobId, CONSOLE_OFFSET, tableName, partitionName);
        kvStoreCache.put(key, offset);
        RetryCommand.run(() -> {
            kvStore.put(key, offset);
            jsonOffsetStore.put(keyForConsole, new ConsoleOffset(offset).toJson());
            return null;
        }, 3);

    }

    private static class OffsetKvSerializer implements IKVSerializer<String, Offset> {

        @Override
        public byte[] serializeValue(Offset value) {
            return SerializerFactory.getKryoSerializer().serialize(value);
        }

        @Override
        public Offset deserializeValue(byte[] valueArray) {
            return (Offset) SerializerFactory.getKryoSerializer().deserialize(valueArray);
        }

        @Override
        public byte[] serializeKey(String key) {
            return key.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserializeKey(byte[] array) {
            return new String(array, StandardCharsets.UTF_8);
        }
    }

    public static String generateKey(String... strings) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                sb.append(KEY_SEPARATOR);
            }
            sb.append(strings[i]);
        }
        return sb.toString().replaceAll("'", "");
    }

    public static class ConsoleOffset {

        private final long offset;

        private final long writeTime;

        enum TYPE {
            TIMESTAMP, NON_TIMESTAMP
        }

        final TYPE type;

        public ConsoleOffset(Offset offset) {
            this.offset = offset.getOffset();
            this.writeTime = System.currentTimeMillis();
            this.type = offset.isTimestamp() ? TYPE.TIMESTAMP : TYPE.NON_TIMESTAMP;
        }

        public String toJson() {
            Map<String, String> kvMap = new HashMap<>(3);
            kvMap.put("offset", String.valueOf(offset));
            kvMap.put("writeTime", String.valueOf(writeTime));
            kvMap.put("type", String.valueOf(type));
            return JSON.toJSONString(kvMap);
        }
    }
}
