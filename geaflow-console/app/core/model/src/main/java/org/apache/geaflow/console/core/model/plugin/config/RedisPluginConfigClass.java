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

package org.apache.geaflow.console.core.model.plugin.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;
import redis.clients.jedis.Jedis;

@Getter
@Setter
@ToString
public class RedisPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.store.redis.host", comment = "i18n.key.host")
    @GeaflowConfigValue(required = true, defaultValue = "0.0.0.0")
    private String host;

    @GeaflowConfigKey(value = "geaflow.store.redis.port", comment = "i18n.key.port")
    @GeaflowConfigValue(required = true, defaultValue = "6379")
    private Integer port;

    @GeaflowConfigKey(value = "geaflow.store.redis.user", comment = "i18n.key.user")
    private String user;

    @GeaflowConfigKey(value = "geaflow.store.redis.password", comment = "i18n.key.password")
    private String password;

    @GeaflowConfigKey(value = "geaflow.store.redis.connection.timeout", comment = "i18n.key.connection.timeout")
    @GeaflowConfigValue(defaultValue = "5000")
    private Integer connectionTimeoutMs;

    @GeaflowConfigKey(value = "geaflow.store.redis.retry.times", comment = "i18n.key.retry.times")
    @GeaflowConfigValue(defaultValue = "10")
    private Integer retryTimes;

    @GeaflowConfigKey(value = "geaflow.store.redis.retry.interval.ms", comment = "i18n.key.retry.interval.ms")
    @GeaflowConfigValue(defaultValue = "500")
    private Integer retryIntervalMs;

    public RedisPluginConfigClass() {
        super(GeaflowPluginType.REDIS);
    }

    @Override
    public void testConnection() {
        try (Jedis jedis = new Jedis(host, port)) {
            jedis.ping();

        } catch (Exception e) {
            throw new GeaflowIllegalException("Redis connection test failed, caused by {}", e.getMessage(), e);
        }
    }
}
