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

package org.apache.geaflow.store.jdbc;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class JdbcConfigKeys {

    public static final ConfigKey JSON_CONFIG = ConfigKeys
        .key("geaflow.store.jdbc.connect.config.json")
        .defaultValue("{}")
        .description("geaflow jdbc json config");

    public static final ConfigKey JDBC_USER_NAME = ConfigKeys
        .key("geaflow.store.jdbc.user.name")
        .defaultValue("")
        .description("geaflow store jdbc user name");

    public static final ConfigKey JDBC_DRIVER_CLASS = ConfigKeys
        .key("geaflow.store.jdbc.driver.class")
        .defaultValue("com.mysql.jdbc.Driver")
        .description("geaflow store jdbc driver class name");

    public static final ConfigKey JDBC_URL = ConfigKeys
        .key("geaflow.store.jdbc.url")
        .noDefaultValue()
        .description("geaflow store jdbc url");

    public static final ConfigKey JDBC_PASSWORD = ConfigKeys
        .key("geaflow.store.jdbc.password")
        .defaultValue("")
        .description("geaflow store jdbc password");

    public static final ConfigKey JDBC_MAX_RETRIES = ConfigKeys
        .key("geaflow.store.jdbc.max.retries")
        .defaultValue(3)
        .description("geaflow store jdbc max retry");

    public static final ConfigKey JDBC_CONNECTION_POOL_SIZE = ConfigKeys
        .key("geaflow.store.jdbc.connection.pool.size")
        .defaultValue(10)
        .description("geaflow store jdbc connection pool size");

    public static final ConfigKey JDBC_PK = ConfigKeys
        .key("geaflow.store.jdbc.pk")
        .defaultValue("pk")
        .description("geaflow store jdbc db pk");

    public static final ConfigKey JDBC_INSERT_FORMAT = ConfigKeys
        .key("geaflow.store.jdbc.insert.format")
        .defaultValue("INSERT INTO %s(%s,%s, gmt_create, gmt_modified) VALUES (?, %s, now(), now())")
        .description("geaflow store jdbc insert format");

    public static final ConfigKey JDBC_UPDATE_FORMAT = ConfigKeys
        .key("geaflow.store.jdbc.update.format")
        .defaultValue("UPDATE %s SET %s=? , gmt_modified=now() WHERE %s='%s'")
        .description("geaflow store jdbc update format");

    public static final ConfigKey JDBC_DELETE_FORMAT = ConfigKeys
        .key("geaflow.store.jdbc.delete.format")
        .defaultValue("DELETE FROM %s WHERE %s='%s'")
        .description("geaflow store jdbc delete format");

    public static final ConfigKey JDBC_QUERY_FORMAT = ConfigKeys
        .key("geaflow.store.jdbc.query.format")
        .defaultValue("SELECT %s FROM %s WHERE %s='%s'")
        .description("geaflow store jdbc query format");
}
