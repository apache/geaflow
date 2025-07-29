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

package org.apache.geaflow.console.core.service.store.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.geaflow.console.common.util.ThreadUtil;
import org.apache.geaflow.console.core.model.job.config.HaMetaArgsClass;
import org.apache.geaflow.console.core.model.plugin.config.RedisPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.store.GeaflowHaMetaStore;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

@Slf4j
@Component
public class RedisStore implements GeaflowHaMetaStore {

    private static final Integer MAX_SCAN_COUNT = Integer.MAX_VALUE;

    private static final int DEFAULT_RETRY_INTERVAL_MS = 500;

    private static final int DEFAULT_RETRY_TIMES = 10;

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5000;

    private static final Cache<String, JedisPool> JEDIS_POOL_CACHE =
        CacheBuilder.newBuilder().initialCapacity(10).maximumSize(100)
            .removalListener(
                (RemovalListener<String, JedisPool>) removalNotification -> {
                    JedisPool jedisPool = removalNotification.getValue();
                    // async clean the resources
                    CompletableFuture.runAsync(() -> {
                        Jedis jedis = jedisPool.getResource();
                        log.info("Jedis pool closed: {}:{}", jedis.getClient().getHost(),
                            jedis.getClient().getPort());
                        jedisPool.close();
                    });
                })
            .build();

    @Override
    public void cleanHaMeta(GeaflowTask task) {
        RedisPluginConfigClass redisConfig = (RedisPluginConfigClass) new HaMetaArgsClass(
            task.getHaMetaPluginConfig()).getPlugin();

        String keyPattern = String.format("*%s*", task.getId());
        deleteByKeyPattern(redisConfig, keyPattern);
    }

    private void deleteByKey(RedisPluginConfigClass config, String key) {
        Jedis jedis = getJedis(config);
        jedis.del(key);
    }

    private void deleteByKeyPattern(RedisPluginConfigClass config, String keyPattern) {
        int retryTimes = Optional.ofNullable(config.getRetryTimes()).orElse(DEFAULT_RETRY_TIMES);
        int retryIntervalMs = Optional.ofNullable(config.getRetryIntervalMs()).orElse(DEFAULT_RETRY_INTERVAL_MS);
        Set<String> keys = deleteByKeyPatternWithRetry(config, keyPattern, retryTimes,
            retryIntervalMs);
        log.info("Successfully deleted redis data with key pattern: {}. Redis host: {}:{}. "
                + "Deleted keys: {}.",
            keyPattern, config.getHost(), config.getHost(), keys);
    }

    private Set<String> deleteByKeyPatternWithRetry(RedisPluginConfigClass config,
                                                    String keyPattern, int retry,
                                                    int retryIntervalMs) {
        try (Jedis jedis = getJedis(config)) {
            Set<String> keySets = getScanKeySets(jedis, keyPattern, MAX_SCAN_COUNT);
            keySets.forEach(jedis::del);
            return keySets;
        } catch (Exception e) {
            if (retry <= 0) {
                throw e;
            }
            ThreadUtil.sleepMilliSeconds(retryIntervalMs);
            return deleteByKeyPatternWithRetry(config, keyPattern, retry - 1, retryIntervalMs);
        }
    }

    private Set<String> getScanKeySets(Jedis jedis, String keyPattern, Integer count) {
        Set<String> keySets = new HashSet<>();
        ScanParams scanParams = new ScanParams();
        scanParams.match(keyPattern);
        scanParams.count(count);
        // the start cursor
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanResult<String> scanResult;
        while (true) {
            scanResult = jedis.scan(cursor, scanParams);
            List<String> result = scanResult.getResult();
            if (CollectionUtils.isNotEmpty(result)) {
                keySets.addAll(result);
            }
            if ("0".equals(cursor)) {
                break;
            }
        }
        return keySets;
    }

    private Jedis getJedis(RedisPluginConfigClass config) {
        String cacheKey = config.toString();
        JedisPool jedisPool = JEDIS_POOL_CACHE.getIfPresent(cacheKey);
        if (jedisPool == null) {
            String host = config.getHost();
            int port = config.getPort();
            int timeout = Optional.ofNullable(config.getConnectionTimeoutMs()).orElse(DEFAULT_CONNECTION_TIMEOUT_MS);
            log.info("Jedis pool created: {}", config);
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            jedisPool = new JedisPool(poolConfig, host, port, timeout, config.getUser(),
                config.getPassword());
            JEDIS_POOL_CACHE.put(cacheKey, jedisPool);
        }
        return jedisPool.getResource();
    }
}
