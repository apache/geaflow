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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.state.serializer.IKMapSerializer;
import org.apache.geaflow.store.api.key.IKMapStore;
import org.apache.geaflow.store.context.StoreContext;
import redis.clients.jedis.Jedis;

public class KMapRedisStore<K, UK, UV> extends BaseRedisStore implements IKMapStore<K, UK, UV> {

    private IKMapSerializer<K, UK, UV> kMapSerializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.kMapSerializer = (IKMapSerializer<K, UK, UV>) Preconditions.checkNotNull(
            storeContext.getKeySerializer(), "keySerializer must be set");
    }

    @Override
    public void add(K key, Map<UK, UV> value) {
        Map<byte[], byte[]> newMap = new HashMap<>(value.size());
        for (Entry<UK, UV> entry : value.entrySet()) {
            byte[] ukArray = this.kMapSerializer.serializeUK(entry.getKey());
            byte[] uvArray = this.kMapSerializer.serializeUV(entry.getValue());
            newMap.put(ukArray, uvArray);
        }
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(redisKey, newMap);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    @Override
    public void add(K key, UK uk, UV value) {
        byte[] ukArray = this.kMapSerializer.serializeUK(uk);
        byte[] uvArray = this.kMapSerializer.serializeUV(value);
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(redisKey, ukArray, uvArray);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    @Override
    public Map<UK, UV> get(K key) {
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        Map<byte[], byte[]> map = RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.hgetAll(redisKey);
            }
        }, retryTimes, retryIntervalMs);

        Map<UK, UV> newMap = new HashMap<>(map.size());
        for (Entry<byte[], byte[]> entry : map.entrySet()) {
            newMap.put(this.kMapSerializer.deserializeUK(entry.getKey()),
                this.kMapSerializer.deserializeUV(entry.getValue()));
        }
        return newMap;
    }

    @Override
    public List<UV> get(K key, UK... uks) {
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);
        byte[][] ukArray = Arrays.stream(uks).map(this.kMapSerializer::serializeUK)
            .toArray(byte[][]::new);

        List<byte[]> uvArray = RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.hmget(redisKey, ukArray);
            }
        }, retryTimes, retryIntervalMs);

        return uvArray.stream().map(this.kMapSerializer::deserializeUV)
            .collect(Collectors.toList());
    }

    @Override
    public void remove(K key) {
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(redisKey);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    @Override
    public void remove(K key, UK... uks) {
        byte[] keyArray = this.kMapSerializer.serializeKey(key);
        byte[] redisKey = getRedisKey(keyArray);
        byte[][] ukArray = Arrays.stream(uks).map(this.kMapSerializer::serializeUK)
            .toArray(byte[][]::new);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hdel(redisKey, ukArray);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }
}
