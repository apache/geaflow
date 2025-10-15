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

package org.apache.geaflow.dsl.connector.jdbc;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class JDBCConfigKeys {
    public static final ConfigKey GEAFLOW_DSL_JDBC_DRIVER = ConfigKeys
        .key("geaflow.dsl.jdbc.driver")
        .noDefaultValue()
        .description("The JDBC driver.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_URL = ConfigKeys
        .key("geaflow.dsl.jdbc.url")
        .noDefaultValue()
        .description("The database URL.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_USERNAME = ConfigKeys
        .key("geaflow.dsl.jdbc.username")
        .noDefaultValue()
        .description("The database username.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PASSWORD = ConfigKeys
        .key("geaflow.dsl.jdbc.password")
        .noDefaultValue()
        .description("The database password.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.jdbc.table.name")
        .noDefaultValue()
        .description("The table name.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PARTITION_NUM = ConfigKeys
        .key("geaflow.dsl.jdbc.partition.num")
        .defaultValue(1L)
        .description("The JDBC partition number, default 1.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PARTITION_COLUMN = ConfigKeys
        .key("geaflow.dsl.jdbc.partition.column")
        .defaultValue("id")
        .description("The JDBC partition column.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PARTITION_LOWERBOUND = ConfigKeys
        .key("geaflow.dsl.jdbc.partition.lowerbound")
        .defaultValue(0L)
        .description("The lowerbound of JDBC partition, just used to decide the partition stride, "
            + "not for filtering the rows in table.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PARTITION_UPPERBOUND = ConfigKeys
        .key("geaflow.dsl.jdbc.partition.upperbound")
        .defaultValue(0L)
        .description("The upperbound of JDBC partition, just used to decide the partition stride, "
            + "not for filtering the rows in table.");
}
