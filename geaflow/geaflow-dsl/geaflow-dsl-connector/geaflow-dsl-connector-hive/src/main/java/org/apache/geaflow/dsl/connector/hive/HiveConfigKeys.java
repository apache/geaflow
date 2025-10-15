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

package org.apache.geaflow.dsl.connector.hive;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class HiveConfigKeys {
    public static final ConfigKey GEAFLOW_DSL_HIVE_DATABASE_NAME = ConfigKeys
        .key("geaflow.dsl.hive.database.name")
        .noDefaultValue()
        .description("The database name for hive table.");

    public static final ConfigKey GEAFLOW_DSL_HIVE_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.hive.table.name")
        .noDefaultValue()
        .description("The hive table name to read.");

    public static final ConfigKey GEAFLOW_DSL_HIVE_METASTORE_URIS = ConfigKeys
        .key("geaflow.dsl.hive.metastore.uris")
        .noDefaultValue()
        .description("The hive meta store uri.");

    public static final ConfigKey GEAFLOW_DSL_HIVE_PARTITION_MIN_SPLITS = ConfigKeys
        .key("geaflow.dsl.hive.splits.per.partition")
        .defaultValue(1)
        .description("The split number of each hive partition.");
}
