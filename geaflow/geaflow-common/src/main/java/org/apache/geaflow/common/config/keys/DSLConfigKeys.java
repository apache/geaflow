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

public class DSLConfigKeys implements Serializable {

    private static final long serialVersionUID = 3550044668482560581L;

    public static final ConfigKey INCR_TRAVERSAL_ITERATION_THRESHOLD = ConfigKeys
        .key("geaflow.dsl.incr.traversal.iteration.threshold")
        .defaultValue(4)
        .description("The max iteration to enable incr match");

    public static final ConfigKey TABLE_SINK_SPLIT_LINE = ConfigKeys
        .key("geaflow.dsl.table.sink.split.line")
        .noDefaultValue()
        .description("The file sink split line.");

    public static final ConfigKey ENABLE_INCR_TRAVERSAL = ConfigKeys
        .key("geaflow.dsl.graph.enable.incr.traversal")
        .defaultValue(false)
        .description("Enable incr match");

    public static final ConfigKey INCR_TRAVERSAL_WINDOW = ConfigKeys
        .key("geaflow.dsl.graph.incr.traversal.window")
        .defaultValue(-1L)
        .description("When window id is large than this parameter to enable incr match");

    public static final ConfigKey GEAFLOW_DSL_STORE_TYPE = ConfigKeys
        .key("geaflow.dsl.graph.store.type")
        .noDefaultValue()
        .description("The graph store type.");

    public static final ConfigKey GEAFLOW_DSL_STORE_SHARD_COUNT = ConfigKeys
        .key("geaflow.dsl.graph.store.shard.count")
        .defaultValue(2)
        .description("The graph store shard count.");

    public static final ConfigKey GEAFLOW_DSL_WINDOW_SIZE = ConfigKeys
        .key("geaflow.dsl.window.size")
        .defaultValue(1L)
        .description("Window size, -1 represent the all window.");

    public static final ConfigKey GEAFLOW_DSL_TIME_WINDOW_SIZE = ConfigKeys
        .key("geaflow.dsl.time.window.size")
        .defaultValue(-1L)
        .description("Specifies source time window size in second unites");

    public static final ConfigKey GEAFLOW_DSL_TABLE_TYPE = ConfigKeys
        .key("geaflow.dsl.table.type")
        .noDefaultValue()
        .description("The table type.");

    public static final ConfigKey GEAFLOW_DSL_TABLE_PARALLELISM = ConfigKeys
        .key("geaflow.dsl.table.parallelism")
        .defaultValue(1)
        .description("The table parallelism.");

    public static final ConfigKey GEAFLOW_DSL_MAX_TRAVERSAL = ConfigKeys
        .key("geaflow.dsl.max.traversal")
        .defaultValue(64)
        .description("The max traversal count.");

    public static final ConfigKey GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION = ConfigKeys
        .key("geaflow.dsl.custom.source.function")
        .noDefaultValue()
        .description("Custom source function class name.");

    public static final ConfigKey GEAFLOW_DSL_CUSTOM_SINK_FUNCTION = ConfigKeys
        .key("geaflow.dsl.custom.sink.function")
        .noDefaultValue()
        .description("Custom sink function class name.");

    public static final ConfigKey GEAFLOW_DSL_QUERY_PATH = ConfigKeys
        .key("geaflow.dsl.query.path")
        .noDefaultValue()
        .description("The gql query path.");

    public static final ConfigKey GEAFLOW_DSL_QUERY_PATH_TYPE = ConfigKeys
        .key("geaflow.dsl.query.path.type")
        .noDefaultValue()
        .description("The gql query path type.");

    public static final ConfigKey GEAFLOW_DSL_PARALLELISM_CONFIG_PATH = ConfigKeys
        .key("geaflow.dsl.parallelism.config.path")
        .noDefaultValue()
        .description("The gql query path.");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_TYPE = ConfigKeys
        .key("geaflow.dsl.catalog.type")
        .defaultValue("memory")
        .description("The catalog type name. Optional internal implementations "
            + "include 'memory' and 'console'");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_TOKEN_KEY = ConfigKeys
        .key("geaflow.dsl.catalog.token.key")
        .noDefaultValue()
        .description("The catalog token key set by console platform");

    public static final ConfigKey GEAFLOW_DSL_CATALOG_INSTANCE_NAME = ConfigKeys
        .key("geaflow.dsl.catalog.instance.name")
        .defaultValue("default")
        .description("The default instance name set by console platform");

    public static final ConfigKey GEAFLOW_DSL_SKIP_EXCEPTION = ConfigKeys
        .key("geaflow.dsl.ignore.exception")
        .defaultValue(false)
        .description("If set true, dsl will skip the exception for dirty data.");

    public static final ConfigKey GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE = ConfigKeys
        .key("geaflow.dsl.traversal.all.split.enable")
        .defaultValue(false)
        .description("Whether enable the split of the ids for traversal all. ");

    public static final ConfigKey GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE = ConfigKeys
        .key("geaflow.dsl.compile.physical.plan.enable")
        .defaultValue(true)
        .description("Whether enable compile query physical plan. ");

    public static final ConfigKey GEAFLOW_DSL_SOURCE_PARALLELISM = ConfigKeys
        .key("geaflow.dsl.source.parallelism")
        .noDefaultValue()
        .description("Set source parallelism");
}
