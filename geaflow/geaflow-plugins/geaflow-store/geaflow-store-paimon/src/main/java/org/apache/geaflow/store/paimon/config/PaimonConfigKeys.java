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

package org.apache.geaflow.store.paimon.config;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class PaimonConfigKeys {

    public static final ConfigKey PAIMON_STORE_WAREHOUSE = ConfigKeys
        .key("geaflow.store.paimon.warehouse")
        .defaultValue("file:///tmp/paimon/")
        .description("paimon warehouse, default LOCAL path, now support path prefix: "
            + "[file://], Options for future: [hdfs://, oss://, s3://]");

    public static final ConfigKey PAIMON_STORE_META_STORE = ConfigKeys
        .key("geaflow.store.paimon.meta.store")
        .defaultValue("FILESYSTEM")
        .description("Metastore of paimon catalog, now support [FILESYSTEM]. Options for future: "
            + "[HIVE, JDBC].");

    public static final ConfigKey PAIMON_STORE_OPTIONS = ConfigKeys
        .key("geaflow.store.paimon.options")
        .defaultValue(128)
        .description("paimon memtable size, default 256MB");

    public static final ConfigKey PAIMON_STORE_DATABASE = ConfigKeys
        .key("geaflow.store.paimon.database")
        .defaultValue("graph")
        .description("paimon graph store database");

    public static final ConfigKey PAIMON_STORE_VERTEX_TABLE = ConfigKeys
        .key("geaflow.store.paimon.vertex.table")
        .defaultValue("vertex")
        .description("paimon graph store vertex table name");

    public static final ConfigKey PAIMON_STORE_EDGE_TABLE = ConfigKeys
        .key("geaflow.store.paimon.edge.table")
        .defaultValue("edge")
        .description("paimon graph store edge table name");

    public static final ConfigKey PAIMON_STORE_INDEX_TABLE = ConfigKeys
        .key("geaflow.store.paimon.index.table")
        .defaultValue("index")
        .description("paimon graph store index table name");

    public static final ConfigKey PAIMON_STORE_DISTRIBUTED_MODE_ENABLE = ConfigKeys
        .key("geaflow.store.paimon.distributed.mode.enable")
        .defaultValue(true)
        .description("paimon graph store distributed mode");

    public static final ConfigKey PAIMON_STORE_TABLE_AUTO_CREATE_ENABLE = ConfigKeys
        .key("geaflow.store.paimon.table.auto.create")
        .defaultValue(false)
        .description("paimon graph store table auto create");

}
