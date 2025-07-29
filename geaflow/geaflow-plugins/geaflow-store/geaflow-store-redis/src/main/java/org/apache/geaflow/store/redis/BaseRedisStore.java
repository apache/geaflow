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

import com.google.common.primitives.Bytes;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

public abstract class BaseRedisStore implements IBaseStore {

    protected static final Logger LOGGER = LoggerFactory.getLogger(KVRedisStore.class);
    protected static final char REDIS_NAMESPACE_SPLITTER = ':';
    protected transient JedisPool jedisPool;
    protected byte[] prefix;
    protected int retryTimes;
    protected int retryIntervalMs;

    public void init(StoreContext storeContext) {
        Configuration config = storeContext.getConfig();
        this.retryTimes = config.getInteger(RedisConfigKeys.REDIS_RETRY_TIMES);
        this.retryIntervalMs = config.getInteger(RedisConfigKeys.REDIS_RETRY_INTERVAL_MS);
        String host = config.getString(RedisConfigKeys.REDIS_HOST);
        int port = config.getInteger(RedisConfigKeys.REDIS_PORT);
        LOGGER.info("redis connect {}:{}", host, port);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        int connectTimeout = config.getInteger(RedisConfigKeys.REDIS_CONNECT_TIMEOUT);
        String user = config.getString(RedisConfigKeys.REDIS_USER.getKey());
        String password = config.getString(RedisConfigKeys.REDIS_PASSWORD.getKey());
        this.jedisPool = new JedisPool(poolConfig, host, port, connectTimeout, user, password);
        String prefixStr = storeContext.getName() + REDIS_NAMESPACE_SPLITTER;
        this.prefix = prefixStr.getBytes();
    }

    @Override
    public void flush() {

    }

    protected byte[] getRedisKey(byte[] key) {
        return Bytes.concat(prefix, key);
    }

    @Override
    public void close() {
        this.jedisPool.close();
    }
}
