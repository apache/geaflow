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

package org.apache.geaflow.dsl.connector.paimon;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class PaimonConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_PAIMON_WAREHOUSE = ConfigKeys
        .key("geaflow.dsl.paimon.warehouse")
        .noDefaultValue()
        .description("The warehouse path for paimon catalog creation.");

    public static final ConfigKey GEAFLOW_DSL_PAIMON_OPTIONS_JSON = ConfigKeys
        .key("geaflow.dsl.paimon.options.json")
        .noDefaultValue()
        .description("The options json for paimon catalog creation.");

    public static final ConfigKey GEAFLOW_DSL_PAIMON_CONFIGURATION_JSON = ConfigKeys
        .key("geaflow.dsl.paimon.configuration.json")
        .noDefaultValue()
        .description("The configuration json for paimon catalog creation.");

    public static final ConfigKey GEAFLOW_DSL_PAIMON_DATABASE_NAME = ConfigKeys
        .key("geaflow.dsl.paimon.database.name")
        .noDefaultValue()
        .description("The database name for paimon table.");

    public static final ConfigKey GEAFLOW_DSL_PAIMON_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.paimon.table.name")
        .noDefaultValue()
        .description("The paimon table name to read.");

}
