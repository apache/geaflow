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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetaStoreFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetaStoreFactory.class);

    private static final int DEFAULT_SHARD_ID = 0;

    public static <K, V> IClusterMetaKVStore<K, V> create(String name, Configuration configuration) {
        return create(name, DEFAULT_SHARD_ID, configuration);
    }

    public static <K, V> IClusterMetaKVStore<K, V> create(String name, int shardId, Configuration configuration) {
        StoreContext storeContext = new StoreContext(name);
        storeContext.withKeySerializer(new DefaultKVSerializer(null, null));
        storeContext.withConfig(configuration);
        storeContext.withShardId(shardId);

        String backendType = configuration.getString(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE);
        IClusterMetaKVStore<K, V> store = create(StoreType.getEnum(backendType));
        store.init(storeContext);
        return store;
    }

    private static <K, V> IClusterMetaKVStore<K, V> create(StoreType storeType) {
        IClusterMetaKVStore clusterMetaKVStore;
        switch (storeType) {
            case ROCKSDB:
                LOGGER.info("create rocksdb cluster metastore");
                clusterMetaKVStore = new RocksdbClusterMetaKVStore();
                break;
            default:
                LOGGER.info("create memory cluster metastore");
                clusterMetaKVStore = new MemoryClusterMetaKVStore();
        }
        return clusterMetaKVStore;
    }
}
