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

package org.apache.geaflow.stats.sink;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SYSTEM_META_TABLE;

import com.alibaba.fastjson.JSON;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncKvStoreWriter implements IStatsWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncKvStoreWriter.class);
    private static final String DEFAULT_NAMESPACE = "default";

    private final IKVStore<String, String> kvStore;

    public SyncKvStoreWriter(Configuration configuration) {
        this.kvStore = createKvStore(configuration);
    }

    private IKVStore<String, String> createKvStore(Configuration configuration) {
        String namespace = DEFAULT_NAMESPACE;
        if (configuration.contains(SYSTEM_META_TABLE)) {
            namespace = configuration.getString(SYSTEM_META_TABLE);
        }
        StoreContext storeContext = new StoreContext(namespace);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, String.class));
        storeContext.withConfig(configuration);

        String storeType = configuration.getString(ExecutionConfigKeys.STATS_METRIC_STORE_TYPE);
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        IKVStore kvStore = (IKVStore) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        LOGGER.info("create stats store with type:{} namespace:{}", storeType, namespace);
        return kvStore;
    }

    @Override
    public void addMetric(String key, Object value) {
        kvStore.put(key, JSON.toJSONString(value));
        kvStore.flush();
    }

    @Override
    public void close() {
    }

}
