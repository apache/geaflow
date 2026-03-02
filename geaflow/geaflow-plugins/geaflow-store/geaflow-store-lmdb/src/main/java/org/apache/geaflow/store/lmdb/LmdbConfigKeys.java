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

package org.apache.geaflow.store.lmdb;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

/**
 * Configuration keys for LMDB storage backend.
 *
 * <p>This class defines all configuration parameters for the LMDB storage implementation,
 * including database names, checkpoint paths, and LMDB-specific settings.
 *
 * <h2>Core Configuration Keys:</h2>
 * <ul>
 *   <li>{@link #LMDB_MAP_SIZE} - Maximum database size (default: 100GB)</li>
 *   <li>{@link #LMDB_MAX_READERS} - Max concurrent read transactions (default: 126)</li>
 *   <li>{@link #LMDB_SYNC_MODE} - Durability vs performance trade-off (default: META_SYNC)</li>
 * </ul>
 *
 * <h2>Database Names:</h2>
 * <ul>
 *   <li>{@link #DEFAULT_DB} - Default key-value storage</li>
 *   <li>{@link #VERTEX_DB} - Vertex data storage</li>
 *   <li>{@link #EDGE_DB} - Edge data storage</li>
 *   <li>{@link #VERTEX_INDEX_DB} - Vertex index storage</li>
 * </ul>
 *
 * @see org.apache.geaflow.store.lmdb.LmdbClient
 * @see org.apache.geaflow.store.lmdb.BaseLmdbStore
 */
public class LmdbConfigKeys {

    /** Checkpoint directory suffix. */
    public static final String CHK_SUFFIX = "_chk";

    /** Default database name for key-value storage. */
    public static final String DEFAULT_DB = "default";

    /** Vertex database name. */
    public static final String VERTEX_DB = "default";

    /** Edge database name. */
    public static final String EDGE_DB = "e";

    /** Vertex index database name. */
    public static final String VERTEX_INDEX_DB = "v_index";

    // Aliases for compatibility with proxy code (CF = Column Family in RocksDB, DB in LMDB)

    /**
     * Alias for {@link #DEFAULT_DB} (RocksDB compatibility).
     */
    public static final String DEFAULT_CF = DEFAULT_DB;

    /**
     * Alias for {@link #VERTEX_DB} (RocksDB compatibility).
     */
    public static final String VERTEX_CF = VERTEX_DB;

    /**
     * Alias for {@link #EDGE_DB} (RocksDB compatibility).
     */
    public static final String EDGE_CF = EDGE_DB;

    /**
     * Alias for {@link #VERTEX_INDEX_DB} (RocksDB compatibility).
     */
    public static final String VERTEX_INDEX_CF = VERTEX_INDEX_DB;

    /**
     * File dot separator.
     */
    public static final char FILE_DOT = '.';

    /** Prefix for vertex database names. */
    public static final String VERTEX_DB_PREFIX = "v_";

    /** Prefix for edge database names. */
    public static final String EDGE_DB_PREFIX = "e_";

    /**
     * Get checkpoint directory path for a specific checkpoint ID.
     *
     * @param path base database path
     * @param checkpointId checkpoint ID
     * @return checkpoint directory path (e.g., "/path/to/db_chk1")
     */
    public static String getChkPath(String path, long checkpointId) {
        return path + CHK_SUFFIX + checkpointId;
    }

    /**
     * Check if a path is a checkpoint path.
     *
     * @param path path to check
     * @return true if path is a checkpoint path
     */
    public static boolean isChkPath(String path) {
        // tmp file may exist.
        return path.contains(CHK_SUFFIX) && path.indexOf(FILE_DOT) == -1;
    }

    /**
     * Get checkpoint path prefix (path + "_chk").
     *
     * @param path full checkpoint path
     * @return checkpoint prefix without ID
     */
    public static String getChkPathPrefix(String path) {
        int end = path.indexOf(CHK_SUFFIX) + CHK_SUFFIX.length();
        return path.substring(0, end);
    }

    /**
     * Extract checkpoint ID from checkpoint path.
     *
     * @param path full checkpoint path (e.g., "/path/to/db_chk1")
     * @return checkpoint ID (e.g., 1)
     */
    public static long getChkIdFromChkPath(String path) {
        return Long.parseLong(path.substring(path.lastIndexOf("chk") + 3));
    }

    // LMDB-specific configuration keys

    /**
     * Maximum database size (map size) in bytes.
     *
     * <p>This is the maximum size the LMDB database can grow to. The value represents
     * address space allocation, not actual disk usage. Set this to 1.5-2x your expected
     * data size.
     *
     * <p>Default: 107374182400 (100GB)
     *
     * <p>Example:
     * <pre>{@code
     * // For 50GB expected data, set to 100GB
     * config.put(LMDB_MAP_SIZE.getKey(), "107374182400");
     * }</pre>
     */
    public static final ConfigKey LMDB_MAP_SIZE = ConfigKeys
        .key("geaflow.store.lmdb.map.size")
        .defaultValue(107374182400L)  // 100GB default
        .description("LMDB max database size (map size), default 100GB");

    /**
     * Maximum number of concurrent read transactions.
     *
     * <p>Higher values allow more concurrent readers but consume more resources.
     * Each slot uses about 4KB of memory.
     *
     * <p>Default: 126
     *
     * <p>Example:
     * <pre>{@code
     * // For high-concurrency read workloads
     * config.put(LMDB_MAX_READERS.getKey(), "256");
     * }</pre>
     */
    public static final ConfigKey LMDB_MAX_READERS = ConfigKeys
        .key("geaflow.store.lmdb.max.readers")
        .defaultValue(126)
        .description("LMDB max concurrent read transactions, default 126");

    /**
     * Sync mode for durability vs performance trade-off.
     *
     * <p>Supported values:
     * <ul>
     *   <li>SYNC - Full fsync on every commit (safest, slowest)</li>
     *   <li>META_SYNC - Sync metadata only (recommended balance)</li>
     *   <li>NO_SYNC - No explicit sync, rely on OS (fastest, least safe)</li>
     * </ul>
     *
     * <p>Default: META_SYNC
     *
     * <p>Example:
     * <pre>{@code
     * // For production use
     * config.put(LMDB_SYNC_MODE.getKey(), "META_SYNC");
     *
     * // For maximum performance (development/testing)
     * config.put(LMDB_SYNC_MODE.getKey(), "NO_SYNC");
     * }</pre>
     */
    public static final ConfigKey LMDB_SYNC_MODE = ConfigKeys
        .key("geaflow.store.lmdb.sync.mode")
        .defaultValue("META_SYNC")
        .description("LMDB sync mode: SYNC, NO_SYNC, META_SYNC, default META_SYNC");

    /**
     * Disable thread-local storage for read transactions.
     *
     * <p>When enabled (true), read transactions are not stored in thread-local storage.
     * This is useful for thread-pooled workloads where a thread may service multiple
     * transactions.
     *
     * <p>Default: false
     */
    public static final ConfigKey LMDB_NO_TLS = ConfigKeys
        .key("geaflow.store.lmdb.no.tls")
        .defaultValue(false)
        .description("LMDB disable thread-local storage, default false");

    /**
     * Use writable memory map.
     *
     * <p>Use a writable memory map for better write performance. Only available on Linux.
     * May cause data corruption on crashes if not used carefully.
     *
     * <p>Default: false
     */
    public static final ConfigKey LMDB_WRITE_MAP = ConfigKeys
        .key("geaflow.store.lmdb.write.map")
        .defaultValue(false)
        .description("LMDB use writable memory map, default false");

    /**
     * Skip metadata sync (used when SYNC mode is enabled).
     *
     * <p>Default: false
     */
    public static final ConfigKey LMDB_NO_META_SYNC = ConfigKeys
        .key("geaflow.store.lmdb.no.meta.sync")
        .defaultValue(false)
        .description("LMDB skip metadata sync, default false");

    /**
     * Thread pool size for checkpoint cleanup operations.
     *
     * <p>Default: 4
     */
    public static final ConfigKey LMDB_PERSISTENT_CLEAN_THREAD_SIZE = ConfigKeys
        .key("geaflow.store.lmdb.persistent.clean.thread.size")
        .defaultValue(4)
        .description("LMDB persistent clean thread size, default 4");

    /**
     * Graph store partition type.
     *
     * <p>Default: "none" (no partitioning)
     */
    public static final ConfigKey LMDB_GRAPH_STORE_PARTITION_TYPE = ConfigKeys
        .key("geaflow.store.lmdb.graph.store.partition.type")
        .defaultValue("none")    // Default none partition
        .description("LMDB graph store partition type, default none");

    /**
     * Graph store start timestamp for time-based partitioning.
     *
     * <p>Default: "1735660800" (2025-01-01 00:00:00)
     */
    public static final ConfigKey LMDB_GRAPH_STORE_DT_START = ConfigKeys
        .key("geaflow.store.lmdb.graph.store.dt.start")
        .defaultValue("1735660800")    // Default start timestamp 2025-01-01 00:00:00
        .description("LMDB graph store start timestamp for dt partition");

    /**
     * Graph store timestamp cycle for time-based partitioning.
     *
     * <p>Default: "2592000" (30 days)
     */
    public static final ConfigKey LMDB_GRAPH_STORE_DT_CYCLE = ConfigKeys
        .key("geaflow.store.lmdb.graph.store.dt.cycle")
        .defaultValue("2592000")    // Default timestamp cycle 30 days
        .description("LMDB graph store timestamp cycle for dt partition");

    /**
     * Map size utilization warning threshold.
     *
     * <p>When map size utilization exceeds this threshold, a warning is logged.
     * This helps prevent running out of space unexpectedly.
     *
     * <p>Default: 0.9 (90%)
     *
     * <p>Example:
     * <pre>{@code
     * // Warn at 80% utilization
     * config.put(LMDB_MAP_SIZE_WARNING_THRESHOLD.getKey(), "0.8");
     * }</pre>
     */
    public static final ConfigKey LMDB_MAP_SIZE_WARNING_THRESHOLD = ConfigKeys
        .key("geaflow.store.lmdb.map.size.warning.threshold")
        .defaultValue(0.9)  // Warn at 90% utilization
        .description("LMDB map size utilization warning threshold, default 0.9");

    // Aliases for RocksDB compatibility (used in copied proxy code)

    /**
     * Alias for {@link #LMDB_PERSISTENT_CLEAN_THREAD_SIZE}.
     */
    public static final ConfigKey ROCKSDB_PERSISTENT_CLEAN_THREAD_SIZE = LMDB_PERSISTENT_CLEAN_THREAD_SIZE;

    /**
     * Alias for {@link #LMDB_GRAPH_STORE_PARTITION_TYPE}.
     */
    public static final ConfigKey ROCKSDB_GRAPH_STORE_PARTITION_TYPE = LMDB_GRAPH_STORE_PARTITION_TYPE;
}
