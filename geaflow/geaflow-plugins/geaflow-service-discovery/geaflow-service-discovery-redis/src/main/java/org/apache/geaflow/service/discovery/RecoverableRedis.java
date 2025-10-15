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

package org.apache.geaflow.service.discovery;

import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.geaflow.store.redis.BaseRedisStore;
import redis.clients.jedis.Jedis;

public class RecoverableRedis extends BaseRedisStore {
    private ISerializer serializer;
    private String namespace;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.serializer = SerializerFactory.getKryoSerializer();
        this.namespace = storeContext.getName() + BaseRedisStore.REDIS_NAMESPACE_SPLITTER;
    }

    public void setData(String key, byte[] valueArray) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);
        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set(redisKey, valueArray);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    public byte[] getData(String key) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);
        return RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.get(redisKey);
            }
        }, retryTimes, retryIntervalMs);
    }

    public void deleteData(String key) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(redisKey);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    public String getNamespace() {
        return this.namespace;
    }

}

