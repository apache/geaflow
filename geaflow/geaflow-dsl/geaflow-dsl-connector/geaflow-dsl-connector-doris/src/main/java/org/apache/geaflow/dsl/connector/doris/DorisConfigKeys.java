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

package org.apache.geaflow.dsl.connector.doris;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class DorisConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_DORIS_FENODES = ConfigKeys
        .key("geaflow.dsl.doris.fenodes")
        .noDefaultValue()
        .description("The Doris FE http address list (host:httpPort), comma separated. "
            + "Used by the sink for Stream Load. When multiple FEs are given the sink fails over "
            + "to the next FE on a failed request.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_JDBC_URL = ConfigKeys
        .key("geaflow.dsl.doris.jdbc.url")
        .noDefaultValue()
        .description("The Doris query (MySQL protocol) jdbc url, e.g. "
            + "jdbc:mysql://host:9030/database. Used by the source for partitioned reads.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_USERNAME = ConfigKeys
        .key("geaflow.dsl.doris.username")
        .defaultValue("root")
        .description("The Doris username.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_PASSWORD = ConfigKeys
        .key("geaflow.dsl.doris.password")
        .defaultValue("")
        .description("The Doris password.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_DATABASE = ConfigKeys
        .key("geaflow.dsl.doris.database")
        .noDefaultValue()
        .description("The Doris database name.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_TABLE = ConfigKeys
        .key("geaflow.dsl.doris.table")
        .noDefaultValue()
        .description("The Doris table name.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_FORMAT = ConfigKeys
        .key("geaflow.dsl.doris.sink.format")
        .defaultValue("json")
        .description("The Stream Load payload format, csv or json. Default json, which safely "
            + "handles newlines, quotes, backslashes and unicode. Csv is a plain separator split "
            + "without quoting and should only be used when values cannot contain the separators.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_COLUMN_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.doris.sink.column.separator")
        .defaultValue("\t")
        .description("The column separator for csv Stream Load. Default tab.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_LINE_DELIMITER = ConfigKeys
        .key("geaflow.dsl.doris.sink.line.delimiter")
        .defaultValue("\n")
        .description("The line delimiter for csv Stream Load. Default newline.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_MAX_ROWS = ConfigKeys
        .key("geaflow.dsl.doris.sink.batch.rows")
        .defaultValue(10000L)
        .description("Flush the buffer to Doris when the buffered row count reaches this "
            + "threshold. Default 10000.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_MAX_BYTES = ConfigKeys
        .key("geaflow.dsl.doris.sink.batch.bytes")
        .defaultValue(10485760L)
        .description("Flush the buffer to Doris when the buffered byte size reaches this "
            + "threshold. Default 10MB.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SINK_MAX_RETRIES = ConfigKeys
        .key("geaflow.dsl.doris.sink.max.retries")
        .defaultValue(3)
        .description("The max retry times for a Stream Load request. Default 3.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_REQUEST_CONNECT_TIMEOUT_MS = ConfigKeys
        .key("geaflow.dsl.doris.request.connect.timeout.ms")
        .defaultValue(30000)
        .description("The connect timeout in milliseconds for Stream Load. Default 30000.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_REQUEST_READ_TIMEOUT_MS = ConfigKeys
        .key("geaflow.dsl.doris.request.read.timeout.ms")
        .defaultValue(30000)
        .description("The socket read timeout in milliseconds for Stream Load. Default 30000.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SOURCE_PARTITION_NUM = ConfigKeys
        .key("geaflow.dsl.doris.source.partition.num")
        .defaultValue(1L)
        .description("The source partition number for parallel reads. Default 1.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SOURCE_PARTITION_COLUMN = ConfigKeys
        .key("geaflow.dsl.doris.source.partition.column")
        .defaultValue("id")
        .description("The numeric column used to split the source into partitions.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SOURCE_PARTITION_LOWERBOUND = ConfigKeys
        .key("geaflow.dsl.doris.source.partition.lowerbound")
        .defaultValue(0L)
        .description("The lowerbound of the partition column, only used to decide the partition "
            + "stride, not for filtering rows.");

    public static final ConfigKey GEAFLOW_DSL_DORIS_SOURCE_PARTITION_UPPERBOUND = ConfigKeys
        .key("geaflow.dsl.doris.source.partition.upperbound")
        .defaultValue(0L)
        .description("The upperbound of the partition column, only used to decide the partition "
            + "stride, not for filtering rows.");
}
