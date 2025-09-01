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

package org.apache.geaflow.cluster.system;

import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStatefulStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksdbClusterMetaKVStore<K, V> implements IClusterMetaKVStore<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksdbClusterMetaKVStore.class);

    private static final Integer DEFAULT_VERSION = 1;

    private IKVStatefulStore<K, Object> kvStore;
    private transient long version;
    private String name;

    @Override
    public void init(StoreContext storeContext) {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.ROCKSDB.name());
        this.name = storeContext.getName();
        kvStore = (IKVStatefulStore<K, Object>) builder.getStore(DataModel.KV,
            storeContext.getConfig());
        kvStore.init(storeContext);

        // recovery
        long latest = kvStore.recoveryLatest();
        if (latest > 0) {
            LOGGER.info("recovery to latest version {}", latest);
            version = latest + 1;
        } else {
            LOGGER.info("not found any version to recovery");
            version = DEFAULT_VERSION;
        }
    }

    @Override
    public void flush() {
        LOGGER.info("cluster meta {} do flush", name);
        kvStore.archive(version);
        version++;
    }

    @Override
    public V get(K key) {
        return (V) kvStore.get(key);
    }

    @Override
    public void put(K key, V value) {
        kvStore.put(key, value);
    }

    @Override
    public void remove(K key) {
        kvStore.remove(key);
    }

    @Override
    public void close() {
        kvStore.close();
    }
}
