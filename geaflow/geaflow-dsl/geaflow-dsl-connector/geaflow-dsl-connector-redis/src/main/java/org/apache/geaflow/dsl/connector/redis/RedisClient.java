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

package org.apache.geaflow.dsl.connector.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.geaflow.common.config.Configuration;
import redis.clients.jedis.JedisPool;

public final class RedisClient {

    private RedisClient() {
    }

    public static JedisPool createJedisPool(Configuration conf) {
        String host = conf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_HOST);
        int port = conf.getInteger(RedisConfigKeys.GEAFLOW_DSL_REDIS_PORT);
        String user = conf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_USER);
        String password = conf.getString(RedisConfigKeys.GEAFLOW_DSL_REDIS_PASSWORD);
        int timeout = conf.getInteger(RedisConfigKeys.GEAFLOW_DSL_REDIS_CONNECTION_TIMEOUT);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        return new JedisPool(poolConfig, host, port, timeout, user, password);
    }
}
