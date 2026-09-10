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

package org.apache.geaflow.dsl.connector.clickhouse;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class ClickHouseConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_DRIVER = ConfigKeys
        .key("geaflow.dsl.clickhouse.driver")
        .defaultValue("com.clickhouse.jdbc.ClickHouseDriver")
        .description("The ClickHouse JDBC driver class, "
            + "default com.clickhouse.jdbc.ClickHouseDriver.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_URL = ConfigKeys
        .key("geaflow.dsl.clickhouse.url")
        .noDefaultValue()
        .description("The ClickHouse JDBC url, e.g. jdbc:clickhouse://host:8123/database.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_USERNAME = ConfigKeys
        .key("geaflow.dsl.clickhouse.username")
        .defaultValue("default")
        .description("The ClickHouse username, default 'default'.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_PASSWORD = ConfigKeys
        .key("geaflow.dsl.clickhouse.password")
        .defaultValue("")
        .description("The ClickHouse password, default empty.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.clickhouse.table.name")
        .noDefaultValue()
        .description("The ClickHouse table name.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_WRITE_BATCH_SIZE = ConfigKeys
        .key("geaflow.dsl.clickhouse.write.batch.size")
        .defaultValue(1000)
        .description("The number of rows buffered before a batch is flushed to ClickHouse. "
            + "ClickHouse is columnar and strongly prefers bulk inserts, so larger batches "
            + "give much higher write throughput. Default 1000.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_PARTITION_NUM = ConfigKeys
        .key("geaflow.dsl.clickhouse.partition.num")
        .defaultValue(1L)
        .description("The number of source read partitions, default 1.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_PARTITION_COLUMN = ConfigKeys
        .key("geaflow.dsl.clickhouse.partition.column")
        .defaultValue("id")
        .description("The column used to split the source read into partitions.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_PARTITION_LOWERBOUND = ConfigKeys
        .key("geaflow.dsl.clickhouse.partition.lowerbound")
        .defaultValue(0L)
        .description("The lowerbound used to decide the partition stride, "
            + "not for filtering the rows in the table.");

    public static final ConfigKey GEAFLOW_DSL_CLICKHOUSE_PARTITION_UPPERBOUND = ConfigKeys
        .key("geaflow.dsl.clickhouse.partition.upperbound")
        .defaultValue(0L)
        .description("The upperbound used to decide the partition stride, "
            + "not for filtering the rows in the table.");
}
