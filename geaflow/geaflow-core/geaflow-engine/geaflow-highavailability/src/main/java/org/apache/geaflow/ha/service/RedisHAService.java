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

package org.apache.geaflow.ha.service;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.redis.KVRedisStore;

public class RedisHAService extends AbstractHAService {

    @Override
    public void open(Configuration configuration) {
        super.open(configuration);
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        this.kvStore = (KVRedisStore) builder.getStore(DataModel.KV, configuration);
        String namespace =
            configuration.getString(ExecutionConfigKeys.JOB_UNIQUE_ID) + TABLE_PREFIX;
        StoreContext storeContext = new StoreContext(namespace);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, null));
        storeContext.withConfig(configuration);
        this.kvStore.init(storeContext);
    }

    @Override
    public void register(String resourceId, ResourceData resourceData) {
        synchronized (kvStore) {
            kvStore.put(resourceId, resourceData);
        }
    }

    @Override
    protected ResourceData getResourceData(String resourceId) {
        synchronized (kvStore) {
            return kvStore.get(resourceId);
        }
    }

}
