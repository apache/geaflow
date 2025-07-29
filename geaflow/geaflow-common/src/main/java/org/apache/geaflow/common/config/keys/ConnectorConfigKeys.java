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

package org.apache.geaflow.common.config.keys;

import java.io.Serializable;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class ConnectorConfigKeys implements Serializable {

    /*************************************************
     *  Connectors Common Parameters.
     *************************************************/
    public static final ConfigKey GEAFLOW_DSL_LINE_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.line.separator")
        .defaultValue("\n")
        .description("The line separator for split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_COLUMN_SEPARATOR = ConfigKeys
        .key("geaflow.dsl.column.separator")
        .defaultValue(",")
        .description("The column separator for split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_PARTITIONS_PER_SOURCE_PARALLELISM = ConfigKeys
        .key("geaflow.dsl.partitions.per.source.parallelism")
        .defaultValue(1)
        .description("Partitions to read for each source parallelism.");

    public static final ConfigKey GEAFLOW_DSL_COLUMN_TRIM = ConfigKeys
        .key("geaflow.dsl.column.trim")
        .defaultValue(true)
        .description("Whether doing trim operation for column text when split text to columns.");

    public static final ConfigKey GEAFLOW_DSL_START_TIME = ConfigKeys
        .key("geaflow.dsl.start.time")
        .defaultValue("begin")
        .description("Specifies the starting unix timestamp for reading the data table. Format "
            + "must be 'yyyy-MM-dd HH:mm:ss'.");

    public static final ConfigKey GEAFLOW_DSL_CONNECTOR_FORMAT = ConfigKeys
        .key("geaflow.dsl.connector.format")
        .defaultValue("text")
        .description("Specifies the deserialization format for reading from external source like kafka, "
            + "possible option currently: json/text");


    public static final ConfigKey GEAFLOW_DSL_CONNECTOR_FORMAT_JSON_IGNORE_PARSE_ERROR = ConfigKeys
        .key("geaflow.dsl.connector.format.json.ignore-parse-error")
        .defaultValue(false)
        .description("for json format, skip fields and rows with parse errors instead of failing. "
            + "Fields are set to null in case of errors.");

    public static final ConfigKey GEAFLOW_DSL_CONNECTOR_FORMAT_JSON_FAIL_ON_MISSING_FIELD = ConfigKeys
        .key("geaflow.dsl.connector.format.json.fail-on-missing-field")
        .defaultValue(false)
        .description("for json format, whether to fail if a field is missing or not.");

    /*************************************************
     *  FILE Connector Parameters.
     *************************************************/
    public static final ConfigKey GEAFLOW_DSL_FILE_PATH = ConfigKeys
        .key("geaflow.dsl.file.path")
        .noDefaultValue()
        .description("The file path for the file table.");

    public static final ConfigKey GEAFLOW_DSL_FILE_NAME_REGEX = ConfigKeys
        .key("geaflow.dsl.file.name.regex")
        .defaultValue("")
        .description("The regular expression for filtering the files in the path.");

    public static final ConfigKey GEAFLOW_DSL_FILE_FORMAT = ConfigKeys
        .key("geaflow.dsl.file.format")
        .defaultValue("txt")
        .description("The file format to read or write, default value is 'txt'. ");

    public static final ConfigKey GEAFLOW_DSL_SKIP_HEADER = ConfigKeys
        .key("geaflow.dsl.skip.header")
        .defaultValue(false)
        .description("Whether skip the header for csv format.");

    public static final ConfigKey GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD = ConfigKeys
        .key("geaflow.dsl.source.file.parallel.mod")
        .defaultValue(false)
        .description("Whether read single file by index");

    public static final ConfigKey GEAFLOW_DSL_SINK_FILE_COLLISION = ConfigKeys
        .key("geaflow.dsl.sink.file.collision")
        .defaultValue("newfile")
        .description("Whether create new file when collision occurs.");

    public static final ConfigKey GEAFLOW_DSL_FILE_LINE_SPLIT_SIZE = ConfigKeys
        .key("geaflow.dsl.file.line.split.size")
        .defaultValue(-1)
        .description("file line split size set by user");

    public static final ConfigKey GEAFLOW_DSL_SOURCE_ENABLE_UPLOAD_METRICS = ConfigKeys
        .key("geaflow.dsl.source.enable.upload.metrics")
        .defaultValue(true)
        .description("source enable upload metrics");

    public static final ConfigKey GEAFLOW_DSL_SINK_ENABLE_SKIP = ConfigKeys
        .key("geaflow.dsl.sink.enable.skip")
        .defaultValue(false)
        .description("sink enable skip");

}


