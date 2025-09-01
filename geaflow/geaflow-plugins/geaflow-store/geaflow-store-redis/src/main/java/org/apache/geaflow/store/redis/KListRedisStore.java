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

package org.apache.geaflow.store.redis;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.api.key.IKListStore;
import org.apache.geaflow.store.context.StoreContext;
import redis.clients.jedis.Jedis;

public class KListRedisStore<K, V> extends BaseRedisStore implements IKListStore<K, V> {

    private IKVSerializer<K, V> kvSerializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.kvSerializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(
            storeContext.getKeySerializer(), "kvSerializer must be set");
    }

    @Override
    public void add(K key, V... values) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);
        byte[][] bValues = Arrays.stream(values).map(this.kvSerializer::serializeValue)
            .toArray(byte[][]::new);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.lpush(redisKey, bValues);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    @Override
    public List<V> get(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        List<byte[]> valueArray = RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.lrange(redisKey, 0, -1);
            }
        }, retryTimes, retryIntervalMs);

        return valueArray.stream().map(this.kvSerializer::deserializeValue)
            .collect(Collectors.toList());
    }


    @Override
    public void remove(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(redisKey);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }
}
