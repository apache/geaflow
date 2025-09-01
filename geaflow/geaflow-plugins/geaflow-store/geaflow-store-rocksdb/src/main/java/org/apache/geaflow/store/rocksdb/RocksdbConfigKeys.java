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

package org.apache.geaflow.store.rocksdb;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;
import org.apache.geaflow.store.rocksdb.options.DefaultGraphOptions;
import org.rocksdb.util.SizeUnit;

public class RocksdbConfigKeys {

    public static final String CHK_SUFFIX = "_chk";
    public static final String DEFAULT_CF = "default";
    public static final String VERTEX_CF = "default";
    public static final String EDGE_CF = "e";
    public static final String VERTEX_INDEX_CF = "v_index";
    public static final char FILE_DOT = '.';
    public static final String VERTEX_CF_PREFIX = "v_";
    public static final String EDGE_CF_PREFIX = "e_";


    public static String getChkPath(String path, long checkpointId) {
        return path + CHK_SUFFIX + checkpointId;
    }

    public static boolean isChkPath(String path) {
        // tmp file may exist.
        return path.contains(CHK_SUFFIX) && path.indexOf(FILE_DOT) == -1;
    }

    public static String getChkPathPrefix(String path) {
        int end = path.indexOf(CHK_SUFFIX) + CHK_SUFFIX.length();
        return path.substring(0, end);
    }

    public static long getChkIdFromChkPath(String path) {
        return Long.parseLong(path.substring(path.lastIndexOf("chk") + 3));
    }

    public static final ConfigKey ROCKSDB_OPTION_CLASS = ConfigKeys
        .key("geaflow.store.rocksdb.option.class")
        .defaultValue(DefaultGraphOptions.class.getCanonicalName())
        .description("rocksdb option class");

    public static final ConfigKey ROCKSDB_OPTIONS_TABLE_BLOCK_SIZE = ConfigKeys
        .key("geaflow.store.rocksdb.table.block.size")
        .defaultValue(128 * SizeUnit.KB)
        .description("rocksdb table block size, default 128KB");

    public static final ConfigKey ROCKSDB_OPTIONS_TABLE_BLOCK_CACHE_SIZE = ConfigKeys
        .key("geaflow.store.rocksdb.table.block.cache.size")
        .defaultValue(1024 * SizeUnit.MB)
        .description("rocksdb table block cache size, default 1G");

    public static final ConfigKey ROCKSDB_OPTIONS_MAX_WRITER_BUFFER_NUM = ConfigKeys
        .key("geaflow.store.rocksdb.max.write.buffer.number")
        .defaultValue(2)
        .description("rocksdb max write buffer number, default 2");

    public static final ConfigKey ROCKSDB_OPTIONS_WRITER_BUFFER_SIZE = ConfigKeys
        .key("geaflow.store.rocksdb.write.buffer.size")
        .defaultValue(128 * SizeUnit.MB)
        .description("rocksdb write buffer size, default 128MB");

    public static final ConfigKey ROCKSDB_OPTIONS_TARGET_FILE_SIZE = ConfigKeys
        .key("geaflow.store.rocksdb.target.file.size")
        .defaultValue(1024 * SizeUnit.MB)
        .description("rocksdb target file size, default 1GB");

    public static final ConfigKey ROCKSDB_STATISTICS_ENABLE = ConfigKeys
        .key("geaflow.store.rocksdb.statistics.enable")
        .defaultValue(false)
        .description("rocksdb statistics, default false");

    public static final ConfigKey ROCKSDB_TTL_SECOND = ConfigKeys
        .key("geaflow.store.rocksdb.ttl.second")
        .defaultValue(10 * 365 * 24 * 3600)   // 10 years.
        .description("rocksdb default ttl, default never ttl");

    public static final ConfigKey ROCKSDB_PERSISTENT_CLEAN_THREAD_SIZE = ConfigKeys
        .key("geaflow.store.rocksdb.persistent.clean.thread.size")
        .defaultValue(4)
        .description("rocksdb persistent clean thread size, default 4");

    public static final ConfigKey ROCKSDB_GRAPH_STORE_PARTITION_TYPE = ConfigKeys
        .key("geaflow.store.rocksdb.graph.store.partition.type")
        .defaultValue("none")    // Default none partition
        .description("rocksdb graph store partition type, default none");

    public static final ConfigKey ROCKSDB_GRAPH_STORE_DT_START = ConfigKeys
        .key("geaflow.store.rocksdb.graph.store.dt.start")
        .defaultValue("1735660800")    // Default start timestamp 2025-01-01 00:00:00
        .description("rocksdb graph store start timestamp for dt partition");

    public static final ConfigKey ROCKSDB_GRAPH_STORE_DT_CYCLE = ConfigKeys
        .key("geaflow.store.rocksdb.graph.store.dt.cycle")
        .defaultValue("2592000")    // Default timestamp cycle 30 days
        .description("rocksdb graph store start timestamp for dt partition");
}
