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

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class RedisConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_REDIS_HOST = ConfigKeys
        .key("geaflow.dsl.redis.host")
        .defaultValue("127.0.0.1")
        .description("Redis server host.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_PORT = ConfigKeys
        .key("geaflow.dsl.redis.port")
        .defaultValue(6379)
        .description("Redis server port.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_USER = ConfigKeys
        .key("geaflow.dsl.redis.user")
        .defaultValue("")
        .description("Redis user name.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_PASSWORD = ConfigKeys
        .key("geaflow.dsl.redis.password")
        .defaultValue("")
        .description("Redis password.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_CONNECTION_TIMEOUT = ConfigKeys
        .key("geaflow.dsl.redis.connection.timeout")
        .defaultValue(5000)
        .description("Redis connection timeout in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_DATA_TYPE = ConfigKeys
        .key("geaflow.dsl.redis.data.type")
        .defaultValue("string")
        .description("Redis data type. Supported values are string and hash.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_KEY_FIELD = ConfigKeys
        .key("geaflow.dsl.redis.key.field")
        .defaultValue("redis_key")
        .description("Table field used as Redis key.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_VALUE_FIELD = ConfigKeys
        .key("geaflow.dsl.redis.value.field")
        .defaultValue("redis_value")
        .description("Table field used as Redis string value.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_HASH_FIELD_FIELD = ConfigKeys
        .key("geaflow.dsl.redis.hash.field.field")
        .defaultValue("")
        .description("Table field used as Redis hash field name.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_HASH_VALUE_FIELD = ConfigKeys
        .key("geaflow.dsl.redis.hash.value.field")
        .defaultValue("")
        .description("Table field used as Redis hash field value.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_KEY_PATTERN = ConfigKeys
        .key("geaflow.dsl.redis.key.pattern")
        .defaultValue("*")
        .description("Redis key pattern used by source scan.");

    public static final ConfigKey GEAFLOW_DSL_REDIS_SCAN_COUNT = ConfigKeys
        .key("geaflow.dsl.redis.scan.count")
        .defaultValue(100)
        .description("Redis SCAN count per fetch.");
}
